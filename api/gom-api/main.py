import subprocess
import os
import tempfile
import json
from io import StringIO
from fastapi import FastAPI, Request, UploadFile, File, Form, HTTPException
from typing import List, Optional
import pandas as pd
import re
from fastapi.middleware.cors import CORSMiddleware


# Cria a instância da aplicação FastAPI
app = FastAPI()

# libera o frontend no localhost:5173
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  # quem pode acessar
    allow_credentials=True,
    allow_methods=["*"],    # libera todos os métodos (GET, POST, etc.)
    allow_headers=["*"],    # libera todos os headers
)

def desconcatena_vars(string_vars: Optional[str]) -> List[str]:
            """
            Desconcatena a string de variáveis separadas por vírgula em uma lista
            Remove espaços em branco e entradas vazias
            """
            if not string_vars or not string_vars.strip():
                return []
            
            vars_list = [var.strip() for var in string_vars.split(",")]
            vars_list = [var for var in vars_list if var]
            return vars_list


# Endpoint de verificação de status da API
@app.get("/")
async def home():
    return {"message": "Bem-vindo à sua API. Use o endpoint /upload-data/ para enviar seus dados."}

# Endpoint principal para o upload de dados
@app.post("/upload-data/")
async def processar_dados(
    file: UploadFile = File(...),
    k_initial: int = Form(...),
    k_final: int = Form(...),
    case_id: str = Form(...),
    internal_vars_string: Optional[str] = Form(None),
):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")
        
    try:
        contents = await file.read()
        csv_data = StringIO(contents.decode('utf-8'))
        df = pd.read_csv(csv_data)

        def limpar_dataframe(df):
            df.columns = [col.replace('"', '').replace("'", "").strip() for col in df.columns]
            
           
            for col in df.columns:
                # Converte para string e remove aspas
                df[col] = df[col].astype(str).str.replace('"', '').str.replace("'", "").str.strip()
                
                # Tenta converter para numérico onde possível
                try:
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    pass
            
            return df

        
        df = limpar_dataframe(df)

        if case_id not in df.columns:
            raise HTTPException(
                status_code=400, 
                detail=f"O CSV não contém a coluna '{case_id}'. Colunas disponíveis: {list(df.columns)}"
            )
        
        internal_vars = desconcatena_vars(internal_vars_string)

        if internal_vars:
            missing_vars = [var for var in internal_vars if var not in df.columns]
            print(f"Variáveis para validação: {internal_vars}")
            print(f"Colunas disponíveis no CSV: {list(df.columns)}")
            
            if missing_vars:
                raise HTTPException(
                    status_code=400,
                    detail=f"Variáveis não encontradas no CSV: {', '.join(missing_vars)}. Colunas disponíveis: {list(df.columns)}"
                )

        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, file.filename)

            # Salva o arquivo CSV limpo
            df.to_csv(csv_path, index=False)

            output_file_path = os.path.join(temp_dir, "model_output.json")
            internal_vars_str = ",".join(internal_vars) if internal_vars else ""

            cmd_args = [
                "Rscript",
                "GomRccp_API.R",
                "--file-path", csv_path,
                "--k-initial", str(k_initial),
                "--k-final", str(k_final),
                "--case-id", case_id,
                "--output-path", output_file_path
            ]

            if internal_vars_str:
                cmd_args.extend(["--internal-vars", internal_vars_str])

            result = subprocess.run(cmd_args, capture_output=True, text=True)

            if result.returncode != 0:
                raise HTTPException(
                    status_code=500,
                    detail={
                        "erro": "Falha ao executar script R",
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                        "cmd": " ".join(cmd_args)
                    }
                )

            if not os.path.exists(output_file_path):
                raise HTTPException(status_code=500, detail="O script R não gerou o arquivo de saída.")

            with open(output_file_path, "r") as f:
                r_output = json.load(f)

            return {
                "status": "sucesso",
                "message": "Dados processados com sucesso!",
                "file_name": file.filename,
                "r_output": r_output
            }

    except pd.errors.ParserError:
        raise HTTPException(status_code=400, detail="Arquivo CSV mal formatado.")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Problema de decodificação do CSV.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro inesperado: {str(e)}")

@app.get("/conversao-txt")
async def transformartxt(
    num_k: int,
    internal_vars_string: Optional[str]
):
   
    internal_vars = desconcatena_vars(internal_vars_string)

    match num_k:
        case 2: 
            file_path = "K2/LogGoMK2(1).TXT"

        case 3: 
            file_path = "K3/LogGoMK3(1).TXT"
            
        case 4: 
            file_path = "K4/LogGoMK4(1).TXT"
    

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"Arquivo TXT de origem não encontrado no caminho: {file_path}")
    
    output_dir = "csv_results"
    os.makedirs(output_dir, exist_ok=True) 

   
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    start_idx = None
    for i, line in enumerate(lines):
        if "Lambda-Marginal Frequency Ratio (LMFR)" in line:
            start_idx = i + 2
            break

    if start_idx is None:
        raise HTTPException(status_code=400, detail="Tabela LMFR não encontrada no arquivo.")

    
    table_lines = []
    blank_count = 0
    for line in lines[start_idx:]:
        line_stripped = line.strip()
        
        # Critério de parada: dois espaços em branco consecutivos ou linha começando com '*'
        if not line_stripped:
            blank_count += 1
            if blank_count >= 2:
                break
            continue
        else:
            blank_count = 0
            
        if line_stripped.startswith("*"):
            break
            
        table_lines.append(line_stripped)

    # 3. Parsing das Linhas para 'data'
    data = []
    current_var = None
    
    
    match num_k:
        case 2: 
            cols = ["Variable", "Level", "n", "perc", "k1", "k2", "k1_perc_lj", "k2_perc_lj"]

        case 3: 
            cols = ["Variable", "Level", "n", "perc", "k1", "k2", "k3", "k1_perc_lj", "k2_perc_lj", "k3_perc_lj"]
            
        case 4: 
            cols = ["Variable", "Level", "n", "perc", "k1", "k2", "k3", "k4", "k1_perc_lj", "k2_perc_lj", "k3_perc_lj", "k4_perc_lj"]
    

    for line in table_lines:
        parts = [p for p in re.split(r"\s+", line) if p]

        if not parts:
            continue

        if parts[0] in internal_vars:
            print(internal_vars, internal_vars_string)
            current_var = parts[0]
            
            parts = parts[1:]
        
        match num_k:
            case 2:
                if current_var is not None and len(parts) >= 7:
                    row_data = parts[:7] 
                
                    data.append([current_var] + row_data)
            case 3:
                if current_var is not None and len(parts) >= 9:
                    row_data = parts[:9] 
                    
                    data.append([current_var] + row_data)
            case 4:
                if current_var is not None and len(parts) >= 11:
                    row_data = parts[:11] 

                    data.append([current_var] + row_data)

    # 4. Criação do DataFrame
    try:
        df = pd.DataFrame(data, columns=cols)
    except ValueError as e:
        # Captura o erro específico de número de colunas e fornece mais detalhes
        if "columns passed, passed data had" in str(e):
             raise HTTPException(
                status_code=500, 
                detail=f"Erro de estrutura de dados: O número de campos extraídos da tabela LMFR ({len(cols)}) não corresponde ao esperado. Log: {e}"
            )
        raise

    # 5. Conversão de Tipos
    match num_k:
        case 2:
            for c in ["n", "perc", "k1", "k2", "k1_perc_lj", "k2_perc_lj"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        case 3:
            for c in ["n", "perc", "k1", "k2", "k3", "k1_perc_lj", "k2_perc_lj", "k3_perc_lj"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        case 4:
            for c in ["n", "perc", "k1", "k2", "k3", "k4", "k1_perc_lj", "k2_perc_lj", "k3_perc_lj", "k4_perc_lj"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")

    # 6. Salvamento e Retorno
    output_path = os.path.join(output_dir, "LMFR.csv")
    df.to_csv(output_path, index=False)

    return {
        "status": "sucesso",
        "mensagem": "Tabela LMFR extraída e salva como CSV.",
        "columns": df.columns.tolist(),
        "rows_count": len(df),
        "csv_path": output_path
    }

@app.get("/dados-heatmap")
async def retornarDadosHeatmap():
    # 1. Carregar os dados
    try:
        df = pd.read_csv("csv_results/LMFR.csv")
    except FileNotFoundError:
        return {"error": "Arquivo CSV não encontrado"}, 404

    # 2. Definir as colunas que queremos (Opção 1 - k1 e k2)
    colunas_importantes = ["Variable", "Level", "k1", "k2"]
    df_filtrado = df[colunas_importantes].copy()
    
    # 3. Preparar os rótulos dos eixos
    # Eixo X: Perfis k1 e k2 (colunas)
    x_labels = ["k1", "k2"]
    
    # Eixo Y: Combinação de Variable + Level (linhas)
    y_labels = []
    for _, row in df_filtrado.iterrows():
        y_label = f"{row['Variable']} - {row['Level']}"
        y_labels.append(y_label)
    
    # 4. Criar a estrutura de dados para ECharts [x_index, y_index, value]
    echarts_data = []
    
    for y_index, row in df_filtrado.iterrows():
        # Para k1: [0, y_index, valor_k1]
        echarts_data.append([0, y_index, float(row["k1"])])
        
        # Para k2: [1, y_index, valor_k2]  
        echarts_data.append([1, y_index, float(row["k2"])])

    # 5. Empacotar e retornar o JSON
    response_data = {
        "xAxisLabels": x_labels,  # ['k1', 'k2']
        "yAxisLabels": y_labels,  # ['Var1 - l1', 'Var1 - l2', 'Var1 - l3', ...]
        "data": echarts_data,     # [[0, 0, 0.6461], [1, 0, 0.0], [0, 1, 0.0], ...]
        "valueKey": "Coeficiente GOM"
    }
    
    return response_data


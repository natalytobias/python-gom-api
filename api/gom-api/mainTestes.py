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
        # Acessa o conteúdo do arquivo para validação inicial
        contents = await file.read()
        csv_data = StringIO(contents.decode('utf-8'))
        df = pd.read_csv(csv_data)

        # FUNÇÃO PARA LIMPAR NOMES DE COLUNAS E CONTEÚDO - REMOVE ASPAS E ESPAÇOS
        def limpar_dataframe(df):
            """Remove aspas, espaços extras e caracteres indesejados dos nomes das colunas e do conteúdo"""
            # Limpa os nomes das colunas
            df.columns = [col.replace('"', '').replace("'", "").strip() for col in df.columns]
            
            # Limpa o conteúdo de todas as células (remove aspas e espaços extras)
            for col in df.columns:
                # Converte para string e remove aspas
                df[col] = df[col].astype(str).str.replace('"', '').str.replace("'", "").str.strip()
                
                # Tenta converter para numérico onde possível
                try:
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    # Mantém como string se não for conversível para numérico
                    pass
            
            return df

        # Aplica a limpeza no DataFrame completo
        df = limpar_dataframe(df)

        # DEBUG: Mostra informações sobre o DataFrame
        print("=== DEBUG INFO ===")
        print(f"Shape do DataFrame: {df.shape}")
        print(f"Colunas do DataFrame: {list(df.columns)}")
        print(f"Tipos de dados das colunas: {df.dtypes.to_dict()}")
        print(f"Primeiras 5 linhas:")
        print(df.head())
        print("==================")

        # Validação da coluna de identificação
        if case_id not in df.columns:
            raise HTTPException(
                status_code=400, 
                detail=f"O CSV não contém a coluna '{case_id}'. Colunas disponíveis: {list(df.columns)}"
            )

        # FUNÇÃO PARA DESCONCATENAR A STRING EM VETOR
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

        # Desconcatena a string em vetor
        internal_vars = desconcatena_vars(internal_vars_string)
        print(f"Variáveis internas processadas: {internal_vars}")

        # Validação das variáveis internas
        if internal_vars:
            missing_vars = [var for var in internal_vars if var not in df.columns]
            print(f"Variáveis para validação: {internal_vars}")
            print(f"Colunas disponíveis no CSV: {list(df.columns)}")
            
            if missing_vars:
                raise HTTPException(
                    status_code=400,
                    detail=f"Variáveis não encontradas no CSV: {', '.join(missing_vars)}. Colunas disponíveis: {list(df.columns)}"
                )

        # Resto do código permanece igual...
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

# Endpoint auxiliar para testar envio normal
@app.post("/debug-upload/")
async def debug_upload(
    file: UploadFile = File(...),
    k_initial: int = Form(...),
    k_final: int = Form(...),
    case_id: str = Form(...),
    internal_vars: List[str] = Form(default=[])
):
    contents = await file.read()
    return {
        "file_name": file.filename,
        "file_size": len(contents),
        "k_initial": k_initial,
        "k_final": k_final,
        "case_id": case_id,
        "internal_vars": internal_vars
    }


# Endpoint auxiliar para inspecionar o form recebido
@app.post("/upload-data-debug/")
async def upload_debug(request: Request):
    form = await request.form()
    files = {k: v.filename for k, v in form.items() if isinstance(v, UploadFile)}
    data = {k: v for k, v in form.items() if not isinstance(v, UploadFile)}
    return {"files": files, "data": data}


# Conversão de TXT -> CSV (já existia no seu código)
@app.get("/conversao-txt")
async def transformartxt():
    file_path = "ktwo/LogGoMK2(1).TXT"

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    start_idx = None
    for i, line in enumerate(lines):
        if "Lambda-Marginal Frequency Ratio (LMFR)" in line:
            start_idx = i + 2
            break

    if start_idx is None:
        return {"status": "erro", "mensagem": "Tabela LMFR não encontrada."}

    table_lines = []
    blank_count = 0
    for line in lines[start_idx:]:
        if line.strip() == "":
            blank_count += 1
            if blank_count == 2:
                break
            continue
        else:
            blank_count = 0
        if line.startswith("*"):
            break
        table_lines.append(line.strip())

    data = []
    current_var = None
    for line in table_lines:
        parts = re.split(r"\s+", line)
        if parts[0].startswith("x"):
            current_var = parts[0]
            parts = parts[1:]
        data.append([current_var] + parts)

    cols = ["Variable", "Level", "n", "perc", "k1", "k2", "k1_perc_lj", "k2_perc_lj"]
    df = pd.DataFrame(data, columns=cols)

    for c in ["n", "perc", "k1", "k2", "k1_perc_lj", "k2_perc_lj"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    output_path = "csv_results/LMFR.csv"
    df.to_csv(output_path, index=False)

    return {
        "status": "sucesso",
        "columns": df.columns.tolist(),
        "rows": df.head(10).to_dict(orient="records"),
        "csv_path": output_path
    }

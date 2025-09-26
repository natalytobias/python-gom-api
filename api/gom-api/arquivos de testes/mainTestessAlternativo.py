from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from starlette.concurrency import run_in_threadpool # Importação chave
from typing import List, Optional
import pandas as pd
from pandas.errors import ParserError
from io import StringIO
import os
import tempfile
import subprocess
import json
import re # Opcional, para validação de segurança

# Endpoint principal para o upload de dados
@app.post("/upload-data/")
async def processar_dados(
    file: UploadFile = File(...),
    k_initial: int = Form(...),
    k_final: int = Form(...),
    case_id: str = Form(...),
    internal_vars: Optional[List[str]] = Form(None)
):
    """
    Processa um arquivo CSV, valida os parâmetros e executa um script R 
    em um thread pool separado para evitar bloqueio.
    """
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="O arquivo deve ser um CSV.")
        
    try:
        # 1. Leitura e Decodificação do arquivo (Apenas uma vez)
        contents = await file.read() # Conteúdo binário (bytes)
        csv_data = StringIO(contents.decode('utf-8'))
        
        # 2. Validação inicial do DataFrame
        df = pd.read_csv(csv_data)

        # Validação das colunas de entrada no DataFrame
        if case_id not in df.columns:
            raise HTTPException(status_code=400, detail=f"O CSV não contém a coluna de identificação '{case_id}'.")
        
        if internal_vars:
            # Validação de segurança opcional
            safe_internal_vars = []
            for var in internal_vars:
                if not re.match(r"^[a-zA-Z0-9._]+$", var):
                    raise HTTPException(status_code=400, detail=f"Nome de variável inválido: '{var}'. Use apenas caracteres alfanuméricos, ponto ou underscore.")
                safe_internal_vars.append(var)
                
            missing_vars = [var for var in safe_internal_vars if var not in df.columns]
            
            if missing_vars:
                raise HTTPException(status_code=400, detail=f"As seguintes variáveis de 'internal_vars' não foram encontradas no CSV: {', '.join(missing_vars)}")

            internal_vars = safe_internal_vars # Usa a lista saneada
            
        # 3. Chamada ao script R com subprocess (em um thread pool)
        with tempfile.TemporaryDirectory() as temp_dir:
            csv_path = os.path.join(temp_dir, file.filename)
            
            # Salva o arquivo CSV usando o 'contents' já lido (mais eficiente)
            with open(csv_path, "wb") as f:
                f.write(contents) 

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

            # EXECUÇÃO NÃO-BLOQUEANTE (melhoria essencial de performance)
            result = await run_in_threadpool(
                subprocess.run, 
                cmd_args, 
                capture_output=True, 
                text=True
            )

            if result.returncode != 0:
                raise HTTPException(status_code=500, detail=f"Erro no script R: {result.stderr}")

            # 4. Leitura e retorno do resultado
            if not os.path.exists(output_file_path):
                raise HTTPException(status_code=500, detail="O script R não gerou o arquivo de saída esperado.")
            
            # A leitura de arquivo (I/O bloqueante) também poderia ser movida para o threadpool, 
            # mas para um pequeno JSON, a diferença é mínima.
            with open(output_file_path, "r") as f:
                r_output = json.load(f)
            
            return {
                "status": "sucesso",
                "message": "Dados processados pelo script R com sucesso!",
                "r_output": r_output,
                "file_name": file.filename
            }

    except ParserError:
        raise HTTPException(status_code=400, detail="O arquivo CSV está mal formatado e não pôde ser lido.")
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="Não foi possível decodificar o arquivo CSV. Verifique a codificação (ex: UTF-8).")
    except Exception as e:
        # Garante que qualquer outro erro inesperado seja capturado
        raise HTTPException(status_code=500, detail=f"Ocorreu um erro inesperado: {type(e).__name__}: {str(e)}")
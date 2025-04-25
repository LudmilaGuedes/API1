from fastapi import FastAPI
from sqlalchemy import create_engine, text
from pathlib import Path
import pandas as pd 

host = 'localhost'
port = '3306'
user = 'root'
senha = '2050'
database_name = 'db_escola'

# uvicorn main:app --reload

BASE_DIR = Path(__file__).parent
DATABASE_URL = f'mysql+pymysql://{user}:{senha}@{host}:{port}/{database_name}'
engine = create_engine(DATABASE_URL)

app = FastAPI()

#read - get
@app.get("/")
def home():
    return {"message": "minha primeira api"}

@app.get("/alunos")
def pegar_alunos():
    with engine.begin() as conn:
        result = conn.execute(text("SELECT * FROM tb_alunos"))
        alunos = [row._asdict() for row in result]
    return alunos

@app.get("/alunos_pandas")
def pegar_alunos():
    with engine.begin() as conn:     
        sql = text("SELECT * FROM tb_alunos")
        df = pd.read_sql(sql, con=engine)
        df = df.fillna(0)
    return df.to_dict(orient="records")

@app.get("/enderecos")
def pegar_enderecos():
    with engine.begin() as conn:
        result = conn.execute(text("SELECT * FROM tb_enderecos"))
        enderecos = [row._asdict() for row in result]
    return enderecos

@app.get("/carros")
def pegar_carros():
    with engine.begin() as conn:
        result = conn.execute(text("SELECT * FROM tb_carros"))
        carros = [row._asdict() for row in result]
    return carros

@app.get("/disciplinas")
def pegar_disciplinas():
    with engine.begin() as conn:
        result = conn.execute(text("SELECT * FROM tb_disciplinas"))
        disciplinas = [row._asdict() for row in result]
    return disciplinas

@app.get("/notas")
def pegar_notas():
    with engine.begin() as conn:
        result = conn.execute(text("SELECT * FROM tb_notas"))
        notas = [row._asdict() for row in result]
    return notas



#create - post
@app.post("/inserir-endereco")
def inserirEndereco(endereco:dict):
    with engine.begin() as conn:
        sql = text("""
            INSERT INTO tb_enderecos (cep, endereco, cidade, estado)
            VALUES (:cep, :endereco, :cidade, :estado)
        """)
        conn.execute(sql, endereco)
        return {"message": "sucesso"}

@app.post("/inserir-aluno")  
def cadastrarAluno(params: dict):
    sql = text("""
        INSERT INTO tb_alunos (nome_aluno, email, cep, carro_id)
        VALUES (:nome_aluno, :email, :cep, :carro_id)
    """)
    with engine.begin() as conn:
        conn.execute(sql, params)
    return {"message": "aluno cadastrado com sucesso"}

@app.post("/inserir-disciplina")
def inserirDisciplina(params: dict):
    sql = text("""
        INSERT INTO tb_disciplinas (nome_disciplina, carga, semestre)
        VALUES (:nome_disciplina, :carga, :semestre)
    """)
    with engine.begin() as conn:
        conn.execute(sql, params)
    return {"message": "disciplina inserida com sucesso"}

@app.post("/inserir-carro")
def inserirCarro(params: dict):
    sql = text("""
        INSERT INTO tb_carros (fabricante, modelo, especificacao)
        VALUES (:fabricante, :modelo, :especificacao)
    """)
    with engine.begin() as conn:
        conn.execute(sql, params)
    return {"message": "carro inserido com sucesso"}

@app.post("/inserir-nota")
def inserirNota(params: dict):
    sql = text("""
        INSERT INTO tb_notas (aluno_id, disciplina_id, nota)
        VALUES (:aluno_id, :disciplina_id, :nota)
    """)
    with engine.begin() as conn:
        conn.execute(sql, params)
    return {"message": "nota inserida com sucesso"}



#update - put 
@app.put("/atualizar-endereco")
def atualizarEndereco(endereco: dict):
    with engine.begin() as conn:
        sql = text("""
            UPDATE tb_enderecos
            SET endereco = :endereco,
                cidade = :cidade,
                estado = :estado
            WHERE cep = :cep
        """)
        result = conn.execute(sql, endereco)
    return {"message": "endereco atualizado com sucesso"}

@app.put("/atualizar-aluno")
def atualizarAluno(aluno: dict):
    with engine.begin() as conn:
        sql = text("""
            UPDATE tb_alunos
            SET nome_aluno = :nome_aluno,
                email = :email,
                cep = :cep,
                carro_id = :carro_id
            WHERE id = :id
        """)
        conn.execute(sql, aluno)
    return {"message": "aluno atualizado com sucesso"}

@app.put("/atualizar-carro")
def atualizarCarro(carro: dict):
    with engine.begin() as conn:
        sql = text("""
            UPDATE tb_carros
            SET fabricante = :fabricante,
                modelo = :modelo,
                especificacao = :especificacao
            WHERE id = :id
        """)
        conn.execute(sql, carro)
    return {"message": "carro atualizado com sucesso"}

@app.put("/atualizar-disciplina")
def atualizarDisciplina(disciplina: dict):
    with engine.begin() as conn:
        sql = text("""
            UPDATE tb_disciplinas
            SET nome_disciplina = :nome_disciplina,
                carga = :carga,
                semestre = :semestre
            WHERE id = :id
        """)
        conn.execute(sql, disciplina)
    return {"message": "disciplina atualizada com sucesso"}

@app.put("/atualizar-nota")
def atualizarNota(nota: dict):
    with engine.begin() as conn:
        sql = text("""
            UPDATE tb_notas
            SET nota = :nota
            WHERE aluno_id = :aluno_id AND disciplina_id = :disciplina_id
        """)
        conn.execute(sql, nota)
    return {"message": "nota atualizada com sucesso"}


#delete 
#http://127.0.0.1:8000/deletar-endereco?cep=01001-009
@app.delete("/deletar-endereco")
def deletarEndereco(cep: str):
    with engine.begin() as conn:
        sql = text("""
                   DELETE FROM tb_enderecos WHERE cep = :cep
                   """)
        result = conn.execute(sql, {"cep": cep})
    return {"message": "endereco deletado com sucesso"}

@app.delete("/deletar-aluno")
def deletarAluno(id: int):
    with engine.begin() as conn:
        sql = text("DELETE FROM tb_alunos WHERE id = :id")
        conn.execute(sql, {"id": id})
    return {"message": "aluno deletado com sucesso"}

@app.delete("/deletar-carro")
def deletarCarro(id: int):
    with engine.begin() as conn:
        sql = text("DELETE FROM tb_carros WHERE id = :id")
        conn.execute(sql, {"id": id})
    return {"message": "carro deletado com sucesso"}

@app.delete("/deletar-disciplina")
def deletarDisciplina(id: int):
    with engine.begin() as conn:
        sql = text("DELETE FROM tb_disciplinas WHERE id = :id")
        conn.execute(sql, {"id": id})
    return {"message": "disciplina deletada com sucesso"}

        


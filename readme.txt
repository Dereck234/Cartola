Como Executar
1️⃣ Criar e ativar ambiente virtual (opcional, mas recomendado)

python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

2️⃣ Instalar dependências

pip install -r requirements.txt

3️⃣ Criar o arquivo .env com a string de conexão do MongoDB

MONGO_URI=mongodb://localhost:27017

4️⃣ Rodar o script

python f1_data_collector.py
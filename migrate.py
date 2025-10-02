# Converted from migrate.ipynb
# This file concatenates all code cells in the original notebook in order.


# ---------- Cell 1 (extracted code) ----------

import pandas as pd
from pymongo import MongoClient

# ---- Étape 1 : Charger le CSV ----
def load_csv(file_path):
    df = pd.read_csv('archive/healthcare_dataset.csv')  # <-- Charger ton fichier CSV
    print("✅ CSV chargé avec succès ! Voici toutes les lignes 📜")
    print(df)  # 👈 Affiche toutes les lignes
    return df

# Exemple d'exécution
if __name__ == "__main__":
    load_csv('archive/healthcare_dataset.csv')


# ---------- Cell 2 (extracted code) ----------

# --- Étape 2 : Nettoyer les données ---
def clean_data(df):
    # Supprimer les doublons
    df = df.drop_duplicates()
    # Remplacer les NaN par None (compatible MongoDB)
    df = df.where(pd.notnull(df), None)
    print("✅ Données nettoyées : doublons supprimés, valeurs manquantes remplacées.")
    return df

# ---------- Cell 3 (extracted code) ----------

# --- Étape 2 : Vérifier l'intégrité des données ---
def check_data_quality(df):
    print("\n📊 Vérification des données...")
    print("📌 Colonnes disponibles :", df.columns.tolist())
    print("📈 Types de colonnes :\n", df.dtypes)
    print("🧪 Valeurs manquantes :\n", df.isnull().sum())
    print("📦 Doublons :", df.duplicated().sum())

# ---------- Cell 4 (extracted code) ----------

# --- Étape 3 : Connexion à MongoDB ---
def connect_mongodb(db_name, collection_name):
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client[db_name]
    collection = db[collection_name]
    return collection

# ---------- Cell 5 (extracted code) ----------

# --- Étape 4 : Insérer les données dans MongoDB ---
def insert_data(collection, df):
    data = df.to_dict(orient="records")
    if len(data) == 0:
        print("⚠️ Aucune donnée à insérer.")
        return
    collection.delete_many({})  # Supprime les anciens documents (optionnel)
    collection.insert_many(data)
    print(f"✅ {len(data)} documents insérés avec succès dans MongoDB !")


# ---------- Cell 6 (extracted code) ----------


# --- Étape 5 : Lire les données pour vérifier ---
def read_data(collection):
    print("\n📤 Vérification des données insérées :")
    for doc in collection.find().limit(5):
        print(doc)

# --- Script principal ---
if __name__ == "__main__":
    # 1️⃣ Charger le CSV
    df = load_csv("archive/healthcare_dataset.csv")

    # 2️⃣ Vérifier les données
    check_data_quality(df)

    # 3️⃣ Connexion à MongoDB
    collection = connect_mongodb("healthcare_db", "patients")

    # 4️⃣ Insérer les données
    insert_data(collection, df)

    # 5️⃣ Lire les données depuis MongoDB
    read_data(collection)


# ---------- Cell 7 (extracted code) ----------

try:
    collection = connect_mongodb("healthcare_db", "patients")  # 👈 même nom que ta fonction
    print(f"La collection '{collection.name}' est accessible.")
except Exception as e:
    print(f"❌ Erreur de connexion à MongoDB : {e}")


# ---------- Cell 9 (extracted code) ----------

if __name__ == "__main__":
    # ⚠️ Important : ce chemin doit correspondre au volume monté dans docker-compose
    df = pd.read_csv('archive/healthcare_dataset.csv')  # ✅ plus d'espace inutile ici

    # ✅ On appelle directement la fonction pour obtenir la collection
    collection = connect_mongodb("healthcare_db", "patients")

    # ✅ On insère les données dans MongoDB
    insert_data(collection, df)


# ---------- Cell 11 (extracted code) ----------

# --- Étape 6 : CREATE - Ajouter un nouveau document ---
def create_document(collection, new_doc):
    result = collection.insert_one(new_doc)
    print(f"✅ Nouveau document inséré avec l'ID : {result.inserted_id}")

# --- Étape 7 : READ - Lire les documents ---
def read_documents(collection, limit=5):
    print("\n📥 Lecture des documents dans MongoDB :")
    for doc in collection.find().limit(limit):
        print(doc)

# --- Étape 8 : UPDATE - Modifier un document ---
def update_document(collection, query, new_values):
    result = collection.update_one(query, {"$set": new_values})
    if result.modified_count > 0:
        print("✅ Document mis à jour avec succès !")
    else:
        print("⚠️ Aucun document correspondant trouvé pour la mise à jour.")

# --- Étape 9 : DELETE - Supprimer un document ---
def delete_document(collection, query):
    result = collection.delete_one(query)
    if result.deleted_count > 0:
        print("🗑️ Document supprimé avec succès !")
    else:
        print("⚠️ Aucun document correspondant trouvé pour la suppression.")

# Python_ETL

[![Continous Integration](https://github.com/cmnemoi/python_project_template/actions/workflows/ci.yaml/badge.svg)](https://github.com/cmnemoi/python_project_template/actions/workflows/ci.yaml)
[![codecov](https://codecov.io/github/cmnemoi/Python_ETL/graph/badge.svg?token=31F9TEL4OU)](https://codecov.io/github/cmnemoi/Python_ETL)

L'objectif de ce repository est de créer un ETL (Extract-Transform-Load) rudimentaire en Python.
L'ETL est composé des trois composants suivants : 
* **Extract** : extraction des données sources vers des dataframes Pandas. Ici les données sources sont des fichiers plats (CSV, JSON).
* **Transform** : la partie critique de l'ETL.
    * vérification des contraintes techniques (exemple : type des variables)
    * vérifcation des contraintes fonctionnelles métier (exemple : chaque étude doit avoir un titre)
    * structuration des données : base de données orientée graphe (format NetworkX "edgelist") 
* **Load** : chargement des données dans leur emplacement final. Ici dans un fichier JSON.

L'ETL charge les données à partir d'un dossier défini par l'utilisateur (ici `data`), les traite puis les charge dans le fichier `data.json` :

![data](images/data.png)

On peut également générer un graphe à partir du fichier obtenu à l'aide de `graph.py` : 

![graphe](images/graph.png)

## Insights

Le journal mentionnant le plus de médicaments différents est "Journal of emergency nursing" avec 6 occurences.

## Exécuter

Nécessite Python **3.11**

- `git clone https://github.com/cmnemoi/Python_ETL.git`
- `python3 -m venv .venv`
- `source .venv/bin/activate`
- `pip install -r requirements.txt`
- `python main.py`

## Pistes d'améliorations (pour grosses volumétries de données)
- traitement par batch
- distribuer les tâches de l'ETL sur plusieurs machines
- utiliser un framework plus adapté comme Spark, voire un langage plus rapide comme Scala ?

# Python_ETL

L'objectif de ce repository est de créer un ETL (Extract-Transform-Load) rudimentaire en Python.
L'ETL est composé des trois composants suivants : 
* **Extract** : extraction des données sources vers des dataframes Pandas. Ici les données sources sont des fichiers plats (CSV, JSON).
* **Transform** : la partie critique de l'ETL.
    * vérification des contraintes techniques (exemple : type des variables)
    * vérifcation des contraintes fonctionnelles métier (exemple : chaque étude doit avoir un titre)
    * structuration des données 
        * création d'un référentiel unique (noms des variables)
        * base de données orientée graphe (format NetworkX "edgelist") 
* **Load** : chargement des données dans leur emplacement final. Ici dans un fichier JSON.
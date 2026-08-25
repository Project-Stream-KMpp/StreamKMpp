#!/usr/bin/env bash
# Télécharge le jeu de données Covertype (UCI ML Repository, 581 012 lignes,
# 54 features + 1 label "Cover_Type"). Utilisé par E2 (trade-off m) et E6
# (comparaison de référence) — voir METHODO_StreamKMpp_Spark.md §4.2.
#
# Le fichier brut (~72 Mo décompressé) n'est PAS versionné (cf. .gitignore) :
# chaque membre du groupe le régénère avec ce script.
set -euo pipefail
cd "$(dirname "$0")"

curl -fSL -o covertype.zip "https://archive.ics.uci.edu/static/public/31/covertype.zip"
unzip -o covertype.zip
gunzip -f covtype.data.gz
rm covertype.zip

echo "OK : $(wc -l < covtype.data) lignes dans $(pwd)/covtype.data"

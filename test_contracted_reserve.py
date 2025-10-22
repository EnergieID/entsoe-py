#!/usr/bin/env python3
"""
Test script pour vérifier les corrections du parser contracted_reserve_prices_procured_capacity
Usage: python test_contracted_reserve.py
"""

import pandas as pd
from entsoe import EntsoePandasClient
import os

def test_contracted_reserve_parser():
    """
    Test du parser corrigé pour les structures France et Belgique
    """
    
    # Configuration
    api_key = os.getenv('ENTSOE_API_KEY')
    if not api_key:
        print("❌ ERREUR: Variable d'environnement ENTSOE_API_KEY non définie")
        print("   Définissez-la avec: export ENTSOE_API_KEY='votre_cle_api'")
        return
    
    client = EntsoePandasClient(api_key=api_key)
    
    # Période de test (1 jour)
    start = pd.Timestamp('2024-01-15', tz='Europe/Brussels')
    end = pd.Timestamp('2024-01-16', tz='Europe/Brussels')
    
    print("🧪 Test du parser contracted_reserve_prices_procured_capacity")
    print(f"📅 Période: {start} à {end}")
    print("🎯 Objectif: Tous les pays à 15 minutes")
    print("=" * 60)
    
    # Test 1: France (doit être à 15 min)
    print("\n🇫🇷 Test FRANCE")
    try:
        df_fr = client.query_contracted_reserve_prices_procured_capacity(
            country_code='FR',
            process_type='A51',
            type_marketagreement_type='A01',
            start=start,
            end=end
        )
        
        print(f"✅ France - Nombre de lignes: {len(df_fr)}")
        print(f"📊 Colonnes: {list(df_fr.columns)}")
        print(f"🕐 Index range: {df_fr.index.min()} à {df_fr.index.max()}")
        if len(df_fr) > 1:
            resolution = df_fr.index[1] - df_fr.index[0]
            print(f"⏱️  Résolution: {resolution}")
            if resolution == pd.Timedelta(minutes=15):
                print("✅ RÉSOLUTION CORRECTE: 15 minutes")
            else:
                print(f"❌ RÉSOLUTION INCORRECTE: {resolution} (attendu: 15 minutes)")
        print(f"📈 Aperçu des données:")
        print(df_fr.head(3))
        
    except Exception as e:
        print(f"❌ Erreur France: {e}")
    
    # Test 2: Belgique (doit être à 15 min)
    print("\n🇧🇪 Test BELGIQUE")
    try:
        df_be = client.query_contracted_reserve_prices_procured_capacity(
            country_code='BE',
            process_type='A51',
            type_marketagreement_type='A01',
            start=start,
            end=end
        )
        
        print(f"✅ Belgique - Nombre de lignes: {len(df_be)}")
        print(f"📊 Colonnes: {list(df_be.columns)}")
        print(f"🕐 Index range: {df_be.index.min()} à {df_be.index.max()}")
        if len(df_be) > 1:
            resolution = df_be.index[1] - df_be.index[0]
            print(f"⏱️  Résolution: {resolution}")
            if resolution == pd.Timedelta(minutes=15):
                print("✅ RÉSOLUTION CORRECTE: 15 minutes")
            else:
                print(f"❌ RÉSOLUTION INCORRECTE: {resolution} (attendu: 15 minutes)")
        print(f"📈 Aperçu des données:")
        print(df_be.head(3))
        
    except Exception as e:
        print(f"❌ Erreur Belgique: {e}")
    
    print("\n" + "=" * 60)
    print("🎯 Résumé des tests:")
    print("- France: doit être à 15 minutes (96 lignes)")
    print("- Belgique: doit être à 15 minutes (96 lignes)")
    print("- Les deux doivent avoir la même résolution")

if __name__ == "__main__":
    test_contracted_reserve_parser()

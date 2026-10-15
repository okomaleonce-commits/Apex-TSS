#!/usr/bin/env python3
"""13 tests du calculateur financier ORION COMMERCE / Abidjan.

Exécuter : python3 -m unittest tools/test_calculateur.py -v
(depuis la racine ORION_COMMERCE_ABIDJAN)

Ces tests verrouillent le modèle : coût rendu, contribution, résultat, stock,
cash, et le comportement sous scénario défavorable. Ils garantissent qu'une
modification du calculateur ne change pas silencieusement un chiffre de décision.
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from calculateur import (  # noqa: E402
    CoutRendu,
    Expedition,
    Periode,
    Stress,
    applique_stress,
    calcule_bilan,
    exercice_fictif,
    rapport_exercice,
)


class TestCalculateur(unittest.TestCase):
    def setUp(self):
        self.cout, self.exp, self.per, self.stress = exercice_fictif()
        self.base = calcule_bilan(self.cout, self.exp, self.per)
        c2, e2, p2 = applique_stress(self.cout, self.exp, self.per, self.stress)
        self.defav = calcule_bilan(c2, e2, p2)

    # 1
    def test_cout_rendu_total(self):
        self.assertEqual(self.cout.total(), 3300)

    # 2
    def test_cout_rendu_stresse_plus_20(self):
        self.assertAlmostEqual(self.cout.stresse(0.20).total(), 3960, places=2)

    # 3
    def test_commission_paiement(self):
        self.assertEqual(self.exp.commission(), 160)  # 2 % de 8000

    # 4
    def test_contribution_base(self):
        self.assertEqual(self.base.contribution_par_expedition, 1990)

    # 5
    def test_contribution_defavorable_chute(self):
        self.assertEqual(self.defav.contribution_par_expedition, 186)
        self.assertLess(
            self.defav.contribution_par_expedition,
            self.base.contribution_par_expedition,
        )

    # 6
    def test_echecs_arrondis(self):
        self.assertEqual(self.base.echecs, 2)        # round(25 * 0.08)
        self.assertEqual(self.defav.echecs, 4)       # round(25 * 0.18)

    # 7
    def test_ventes_completees(self):
        self.assertEqual(self.base.ventes_completees, 23)
        self.assertEqual(self.base.ventes_completees + self.base.echecs, 25)

    # 8
    def test_resultat_base_positif(self):
        self.assertEqual(self.base.resultat_avant_fiscalite, 3970)
        self.assertGreater(self.base.resultat_avant_fiscalite, 0)

    # 9
    def test_resultat_defavorable_negatif(self):
        self.assertEqual(self.defav.resultat_avant_fiscalite, -39694)
        self.assertLess(self.defav.resultat_avant_fiscalite, 0)

    # 10
    def test_valeur_stock_base(self):
        # 25 approvisionnées - 23 vendues = 2 en stock, à 3300 le coût rendu
        self.assertEqual(self.base.valeur_stock, 6600)

    # 11
    def test_cash_libre_base(self):
        self.assertEqual(self.base.cash_libre, 43700)

    # 12
    def test_cash_libre_defavorable_negatif(self):
        # Le scénario défavorable casse la trésorerie : réserve indispensable.
        self.assertEqual(self.defav.cash_libre, -20200)
        self.assertLess(self.defav.cash_libre, 0)

    # 13
    def test_perte_stock_si_unites_non_recuperees(self):
        # Si une part des unités en échec est perdue, la perte stock apparaît
        # au résultat ET réduit le stock restant.
        per2 = Periode(
            unites_approvisionnees=self.per.unites_approvisionnees,
            tentatives_livraison=self.per.tentatives_livraison,
            taux_echec=self.per.taux_echec,
            cout_echec_logistique=self.per.cout_echec_logistique,
            part_unites_perdues=0.5,  # la moitié des échecs non récupérés
            charges_fixes=self.per.charges_fixes,
        )
        b = calcule_bilan(self.cout, self.exp, per2)
        self.assertEqual(b.unites_perdues, 1.0)          # round(2 * 0.5)
        self.assertEqual(b.perte_stock, 3300)            # 1 unité * 3300
        self.assertEqual(
            b.resultat_avant_fiscalite,
            self.base.resultat_avant_fiscalite - 3300,
        )


class TestRapport(unittest.TestCase):
    def test_rapport_structure(self):
        rap = rapport_exercice()
        self.assertIn("base", rap)
        self.assertIn("defavorable", rap)
        self.assertIn("resultat_avant_fiscalite", rap["base"])


if __name__ == "__main__":
    unittest.main(verbosity=2)

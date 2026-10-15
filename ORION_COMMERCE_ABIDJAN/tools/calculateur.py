#!/usr/bin/env python3
"""ORION COMMERCE / Abidjan — Calculateur financier.

Objet : avant tout achat, connaître le COÛT COMPLET réellement payé, examiner
un scénario défavorable, fixer un plafond de perte et vérifier que la réserve
tient au pire moment. Le calculateur distingue trois choses que l'on confond
trop souvent :

  - le RÉSULTAT (profit/perte de la période, avant fiscalité) ;
  - le STOCK (valeur immobilisée en marchandise, au coût rendu) ;
  - le CASH (trésorerie réellement disponible), y compris l'avance possible de
    remboursements versés au client avant que la plateforme/le COD ne reverse.

Principe anti-invention (hérité du mandat) : cet outil ne prédit aucun produit
et n'invente aucune donnée. Les chiffres de l'exercice fictif ci-dessous sont
des ENTRÉES de démonstration, choisies pour tester l'outil — pas un devis, pas
une promesse de rentabilité. Le budget réel doit être reconstruit sur les coûts
de formalisation, de l'opérateur et de l'offre retenue.

Dépendances : bibliothèque standard uniquement.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Dict
import argparse
import json


# --------------------------------------------------------------------------- #
# 1. Coût rendu (landed cost) — ce qu'une unité coûte, posée au stock Abidjan   #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CoutRendu:
    """Composantes du coût d'une unité, de l'usine au stock Abidjan (FCFA)."""
    prix_fob: float            # prix marchandise départ fournisseur (FOB)
    transport_chine: float     # acheminement local Chine -> port/aéroport
    inspection: float          # contrôle qualité / échantillon amorti par unité
    fret: float                # transport international
    assurance: float           # assurance transport
    formalites_douane: float   # droits, taxes, dédouanement, par unité
    terminal_stockage: float   # terminal, magasinage, manutention
    transport_au_stock: float  # terminal Abidjan -> lieu de stock
    frais_paiement_change: float  # frais bancaires / change sur l'approvisionnement

    def total(self) -> float:
        return round(
            self.prix_fob
            + self.transport_chine
            + self.inspection
            + self.fret
            + self.assurance
            + self.formalites_douane
            + self.terminal_stockage
            + self.transport_au_stock
            + self.frais_paiement_change,
            2,
        )

    def stresse(self, hausse: float) -> "CoutRendu":
        """Applique une hausse relative uniforme (ex. +0.20) au coût rendu."""
        k = 1.0 + hausse
        return replace(
            self,
            prix_fob=self.prix_fob * k,
            transport_chine=self.transport_chine * k,
            inspection=self.inspection * k,
            fret=self.fret * k,
            assurance=self.assurance * k,
            formalites_douane=self.formalites_douane * k,
            terminal_stockage=self.terminal_stockage * k,
            transport_au_stock=self.transport_au_stock * k,
            frais_paiement_change=self.frais_paiement_change * k,
        )


# --------------------------------------------------------------------------- #
# 2. Économie d'une expédition (une vente livrée et encaissée)                  #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Expedition:
    """Coûts variables d'UNE vente livrée (hors coût rendu de l'unité)."""
    prix_vente: float               # prix total payé par le client
    acquisition_par_commande: float  # publicité / contenus ramenés à la commande
    emballage: float                # emballage et consommables
    operateur_par_commande: float   # part de l'opérateur humain par commande
    livraison_marchand: float       # part de livraison à la charge du marchand
    taux_commission: float          # commission paiement/plateforme (part du prix)
    frais_divers: float = 0.0       # frais résiduels par commande

    def commission(self) -> float:
        return round(self.prix_vente * self.taux_commission, 2)

    def contribution(self, cout_rendu: float) -> float:
        """Marge que la vente laisse pour couvrir les charges fixes."""
        return round(
            self.prix_vente
            - cout_rendu
            - self.acquisition_par_commande
            - self.emballage
            - self.operateur_par_commande
            - self.livraison_marchand
            - self.commission()
            - self.frais_divers,
            2,
        )


# --------------------------------------------------------------------------- #
# 3. Période : résultat, stock, cash                                           #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Periode:
    """Paramètres d'une période-pilote."""
    unites_approvisionnees: int   # stock acheté pour le pilote
    tentatives_livraison: int     # nombre de livraisons tentées
    taux_echec: float             # part des tentatives qui échouent (refus/retour)
    cout_echec_logistique: float  # coût d'un échec : aller + emballage perdus
    part_unites_perdues: float    # part des unités en échec NON récupérées (0..1)
    charges_fixes: float          # charges fixes de la période (formalités amorties, outils, abonnements)
    delai_reversement_jours: int = 0  # info : délai avant reversement des encaissements


@dataclass(frozen=True)
class Bilan:
    cout_rendu_unitaire: float
    contribution_par_expedition: float
    ventes_completees: int
    echecs: int
    unites_perdues: float
    # Résultat (P&L)
    marge_ventes: float
    cout_echecs: float
    perte_stock: float
    charges_fixes: float
    resultat_avant_fiscalite: float
    # Stock
    unites_en_stock: float
    valeur_stock: float
    # Cash
    recettes_encaissees: float
    avance_remboursements: float
    cash_investi_approvisionnement: float
    cash_libre: float

    def as_dict(self) -> Dict[str, float]:
        d = self.__dict__.copy()
        return {k: (round(v, 2) if isinstance(v, float) else v) for k, v in d.items()}


def calcule_bilan(cout: CoutRendu, exp: Expedition, per: Periode) -> Bilan:
    """Fond les trois briques en un bilan résultat / stock / cash."""
    cr = cout.total()
    contrib = exp.contribution(cr)

    echecs = round(per.tentatives_livraison * per.taux_echec)
    ventes = per.tentatives_livraison - echecs
    # Une unité vendue sort du stock ; une unité en échec est perdue (part) ou
    # revient au stock (réutilisable).
    unites_perdues = round(echecs * per.part_unites_perdues, 2)

    marge_ventes = round(ventes * contrib, 2)
    cout_echecs = round(echecs * per.cout_echec_logistique, 2)
    perte_stock = round(unites_perdues * cr, 2)
    resultat = round(marge_ventes - cout_echecs - perte_stock - per.charges_fixes, 2)

    # Stock : approvisionné - vendu - perdu (les échecs récupérés reviennent).
    unites_en_stock = round(per.unites_approvisionnees - ventes - unites_perdues, 2)
    valeur_stock = round(max(unites_en_stock, 0) * cr, 2)

    # Cash : recettes encaissées, moins le cash immobilisé à l'approvisionnement,
    # moins l'avance de remboursements (versée avant reversement).
    recettes = round(ventes * exp.prix_vente, 2)
    avance_remb = round(echecs * exp.prix_vente, 2)  # pire cas : remboursé avant reversement
    cash_appro = round(per.unites_approvisionnees * cr, 2)
    # Cash libre = encaissé - appro - charges fixes - coûts échecs - avance remboursements.
    cash_libre = round(
        recettes - cash_appro - per.charges_fixes - cout_echecs - avance_remb, 2
    )

    return Bilan(
        cout_rendu_unitaire=cr,
        contribution_par_expedition=contrib,
        ventes_completees=ventes,
        echecs=echecs,
        unites_perdues=unites_perdues,
        marge_ventes=marge_ventes,
        cout_echecs=cout_echecs,
        perte_stock=perte_stock,
        charges_fixes=per.charges_fixes,
        resultat_avant_fiscalite=resultat,
        unites_en_stock=unites_en_stock,
        valeur_stock=valeur_stock,
        recettes_encaissees=recettes,
        avance_remboursements=avance_remb,
        cash_investi_approvisionnement=cash_appro,
        cash_libre=cash_libre,
    )


# --------------------------------------------------------------------------- #
# 4. Scénario défavorable (stress)                                             #
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class Stress:
    """Stress documenté dans le guide : +coût rendu, -prix, +acquisition, +échecs."""
    hausse_cout_rendu: float = 0.20     # +20 %
    baisse_prix: float = 0.10           # -10 %
    hausse_acquisition: float = 0.30    # +30 %
    hausse_echecs_points: float = 0.10  # +10 points de taux d'échec


def applique_stress(
    cout: CoutRendu, exp: Expedition, per: Periode, s: Stress
) -> tuple[CoutRendu, Expedition, Periode]:
    cout2 = cout.stresse(s.hausse_cout_rendu)
    exp2 = replace(
        exp,
        prix_vente=exp.prix_vente * (1.0 - s.baisse_prix),
        acquisition_par_commande=exp.acquisition_par_commande * (1.0 + s.hausse_acquisition),
    )
    per2 = replace(per, taux_echec=min(1.0, per.taux_echec + s.hausse_echecs_points))
    return cout2, exp2, per2


# --------------------------------------------------------------------------- #
# 5. Scénario de démonstration (chiffres fictifs, pour tester l'outil)         #
# --------------------------------------------------------------------------- #
def exercice_fictif() -> tuple[CoutRendu, Expedition, Periode, Stress]:
    """Entrées INVENTÉES pour éprouver le calculateur. Ni devis, ni promesse."""
    cout = CoutRendu(
        prix_fob=1800,
        transport_chine=120,
        inspection=80,
        fret=600,
        assurance=60,
        formalites_douane=350,
        terminal_stockage=120,
        transport_au_stock=90,
        frais_paiement_change=80,
    )  # total = 3300
    exp = Expedition(
        prix_vente=8000,
        acquisition_par_commande=1200,
        emballage=300,
        operateur_par_commande=450,
        livraison_marchand=600,
        taux_commission=0.02,
        frais_divers=0,
    )
    per = Periode(
        unites_approvisionnees=25,
        tentatives_livraison=25,
        taux_echec=0.08,
        cout_echec_logistique=900,
        part_unites_perdues=0.0,     # base : unités en échec récupérées
        charges_fixes=40000,
        delai_reversement_jours=7,
    )
    stress = Stress()
    return cout, exp, per, stress


def rapport_exercice() -> Dict[str, Dict[str, float]]:
    cout, exp, per, stress = exercice_fictif()
    base = calcule_bilan(cout, exp, per)
    c2, e2, p2 = applique_stress(cout, exp, per, stress)
    defav = calcule_bilan(c2, e2, p2)
    return {"base": base.as_dict(), "defavorable": defav.as_dict()}


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #
def _fmt(v) -> str:
    if isinstance(v, float):
        return f"{v:,.0f} FCFA".replace(",", " ")
    return str(v)


def main() -> None:
    ap = argparse.ArgumentParser(description="Calculateur financier ORION COMMERCE / Abidjan")
    ap.add_argument("--exercice", action="store_true", help="exécute l'exercice fictif (base + défavorable)")
    ap.add_argument("--json", action="store_true", help="sortie JSON")
    args = ap.parse_args()

    rap = rapport_exercice()
    if args.json:
        print(json.dumps(rap, ensure_ascii=False, indent=2))
        return

    lignes = [
        ("Coût rendu unitaire", "cout_rendu_unitaire"),
        ("Contribution par expédition", "contribution_par_expedition"),
        ("Ventes complétées", "ventes_completees"),
        ("Échecs", "echecs"),
        ("Résultat de période avant fiscalité", "resultat_avant_fiscalite"),
        ("Valeur du stock restant", "valeur_stock"),
        ("Cash libre", "cash_libre"),
    ]
    print("ORION COMMERCE / Abidjan — exercice fictif (chiffres inventés, test de l'outil)")
    print("=" * 74)
    print(f"{'Poste':40} {'Base':>16} {'Défavorable':>16}")
    print("-" * 74)
    for label, key in lignes:
        b = rap["base"][key]
        d = rap["defavorable"][key]
        print(f"{label:40} {_fmt(b):>16} {_fmt(d):>16}")
    print("=" * 74)
    print("Rappel : un bon taux de marge ne suffit pas. Avant achat : coût complet")
    print("connu, scénario défavorable examiné, plafond de perte fixé, réserve tenue.")


if __name__ == "__main__":
    main()

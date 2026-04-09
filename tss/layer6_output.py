"""
TSS LAYER 6 — OUTPUT
====================
Formatage des signaux pour :
  - Telegram (ApexSiriusBot)
  - JSON structuré
  - Dashboard console

Gestion du signal store (historique des signaux émis).
"""

import json
import logging
import os
import requests
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

from tss.layer3_signal_engine import SignalDecision, SignalMetrics, DECISION_STARS
from tss.layer5_risk_engine import StakeResult, ApexAlignment

logger = logging.getLogger("TSS.Layer6")


# ─────────────────────────────────────────────
#  SIGNAL STORE
# ─────────────────────────────────────────────

@dataclass
class TSSSignal:
    """Signal complet émis par le TSS."""
    match_id: str
    home: str
    away: str
    league: str
    kickoff: str
    metrics: dict           # SignalMetrics.to_dict()
    stake: dict             # StakeResult.to_dict()
    apex_alignment: str     # ApexAlignment.value
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    result_actual: Optional[bool] = None   # Renseigné après le match (audit)
    pnl: Optional[float] = None            # P&L en unités

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False)


class SignalStore:
    """Stockage JSONL des signaux émis."""

    def __init__(self, path: str = "data/tss_signals.jsonl"):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)

    def save(self, signal: TSSSignal) -> None:
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(signal.to_json().replace("\n", " ") + "\n")
        logger.info(f"[SignalStore] Signal sauvegardé : {signal.match_id} | {signal.metrics.get('target_market')}")

    def load_all(self) -> list[TSSSignal]:
        if not os.path.exists(self.path):
            return []
        signals = []
        with open(self.path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        d = json.loads(line)
                        signals.append(TSSSignal(**d))
                    except Exception as e:
                        logger.warning(f"Ligne invalide dans SignalStore : {e}")
        return signals

    def update_result(self, match_id: str, target_market: str, actual: bool, pnl: float) -> None:
        """Met à jour le résultat d'un signal après le match."""
        signals = self.load_all()
        updated = []
        for s in signals:
            if s.match_id == match_id and s.metrics.get("target_market") == target_market:
                s.result_actual = actual
                s.pnl = pnl
            updated.append(s)
        # Réécrire le fichier complet
        with open(self.path, "w", encoding="utf-8") as f:
            for s in updated:
                f.write(s.to_json().replace("\n", " ") + "\n")


# ─────────────────────────────────────────────
#  FORMATEUR TELEGRAM
# ─────────────────────────────────────────────

ALIGNMENT_EMOJI = {
    ApexAlignment.ALIGNED.value:    "🟢 ALIGNÉ",
    ApexAlignment.ABSENT.value:     "⚪ ABSENT",
    ApexAlignment.DIVERGENT.value:  "🟡 DIVERGENT",
    ApexAlignment.CONTRADICT.value: "🔴 CONTRADICTION"
}

DECISION_EMOJI = {
    SignalDecision.MAXIMAL.value:  "🔥",
    SignalDecision.STRONG.value:   "✅",
    SignalDecision.MODERATE.value: "📊",
    SignalDecision.WEAK.value:     "📉",
    SignalDecision.NO_BET.value:   "❌"
}


def format_telegram(
    home: str,
    away: str,
    league: str,
    kickoff: str,
    metrics: SignalMetrics,
    stake: StakeResult
) -> str:
    """Génère le message Telegram formaté pour ApexSiriusBot."""

    stars = DECISION_STARS.get(metrics.decision, "")
    dec_emoji = DECISION_EMOJI.get(metrics.decision.value, "")
    align_str = ALIGNMENT_EMOJI.get(stake.alignment.value, stake.alignment.value)
    gate_line = ""
    if metrics.gates_failed:
        gate_line = f"\n⛔ Gates KO   : {' | '.join(metrics.gates_failed)}"
    flag_line = ""
    if metrics.flags:
        flag_line = f"\n🚩 Flags      : {' | '.join(metrics.flags)}"

    stake_line = (
        f"💰 Stake      : {stake.stake_units:.2f}u "
        f"({stake.stake_pct_tss*100:.2f}% TSS Bankroll)"
        if metrics.decision != SignalDecision.NO_BET
        else "💰 Stake      : 0u — NO BET"
    )

    msg = (
        f"🔺 *TSS SIGNAL v1.0*\n"
        f"{'━'*28}\n"
        f"📌 *Match*    : {home} vs {away}\n"
        f"🏆 *Ligue*    : {league}\n"
        f"🗓 *Heure*    : {kickoff}\n"
        f"🎯 *Marché*   : {metrics.target_market.upper()}\n"
        f"\n"
        f"📊 *TRIANGULATION*\n"
        f"├ Module A  : {f'{metrics.p_synth*100:.1f}%' if metrics.p_module_A is None else f'{metrics.p_module_A*100:.1f}%'}\n"
        f"├ Module B  : {f'N/A' if metrics.p_module_B is None else f'{metrics.p_module_B*100:.1f}%'}\n"
        f"└ Module C  : {f'N/A' if metrics.p_module_C is None else f'{metrics.p_module_C*100:.1f}%'}\n"
        f"→ P_synth   : {metrics.p_synth*100:.1f}%\n"
        f"→ P_réelle  : {metrics.p_real*100:.1f}% (cote {metrics.cote})\n"
        f"\n"
        f"📐 *SIGNAL METRICS*\n"
        f"├ Δ         : {metrics.delta*100:+.1f}%\n"
        f"├ IC        : {metrics.ic:.3f}\n"
        f"├ EV        : {metrics.ev*100:+.1f}%\n"
        f"└ SDT       : {metrics.sdt:.3f}\n"
        f"\n"
        f"{stake_line}\n"
        f"\n"
        f"⚡ *APEX-ENGINE* : {align_str}\n"
        f"{gate_line}{flag_line}\n"
        f"{'━'*28}\n"
        f"{dec_emoji} *TSS — {metrics.decision.value}* {stars}\n"
    )
    return msg


# ─────────────────────────────────────────────
#  ENVOI TELEGRAM
# ─────────────────────────────────────────────

class TelegramOutput:
    """Envoie les signaux vers ApexSiriusBot."""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{bot_token}"

    def send(self, message: str) -> bool:
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
        try:
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 200:
                logger.info("[Telegram] Message envoyé avec succès.")
                return True
            else:
                logger.error(f"[Telegram] Erreur {r.status_code}: {r.text}")
                return False
        except Exception as e:
            logger.error(f"[Telegram] Exception : {e}")
            return False


# ─────────────────────────────────────────────
#  OUTPUT ORCHESTRATEUR
# ─────────────────────────────────────────────

class OutputLayer:
    """
    Point d'entrée Layer 6.
    Assemble le signal, le sauvegarde, et diffuse sur Telegram si configuré.
    """

    def __init__(
        self,
        store_path: str = "data/tss_signals.jsonl",
        telegram_token: Optional[str] = None,
        telegram_chat_id: Optional[str] = None
    ):
        self.store = SignalStore(store_path)
        self.telegram = (
            TelegramOutput(telegram_token, telegram_chat_id)
            if telegram_token and telegram_chat_id
            else None
        )

    def emit(
        self,
        home: str,
        away: str,
        league: str,
        kickoff: str,
        metrics: SignalMetrics,
        stake: StakeResult,
        match_id: Optional[str] = None
    ) -> TSSSignal:

        mid = match_id or f"{home.lower().replace(' ', '_')}_vs_{away.lower().replace(' ', '_')}"

        signal = TSSSignal(
            match_id=mid,
            home=home,
            away=away,
            league=league,
            kickoff=kickoff,
            metrics=metrics.to_dict(),
            stake=stake.to_dict(),
            apex_alignment=stake.alignment.value
        )

        # Sauvegarde
        self.store.save(signal)

        # Console
        msg = format_telegram(home, away, league, kickoff, metrics, stake)
        print("\n" + msg)

        # Telegram
        if self.telegram and metrics.decision != SignalDecision.NO_BET:
            self.telegram.send(msg)
        elif self.telegram and metrics.decision == SignalDecision.NO_BET:
            logger.info("[Output] Signal NO_BET — Telegram non sollicité.")

        return signal

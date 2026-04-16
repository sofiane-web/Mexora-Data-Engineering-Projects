"""
utils/logger.py — Configuration du logging centralisé pour le pipeline ETL Mexora
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from config.settings import LOGS_DIR


class ETLLogger:
    """
    Logger ETL avec statistiques de transformation intégrées.
    Enregistre automatiquement le nombre de lignes affectées par chaque règle.
    """

    def __init__(self, name: str = "mexora_etl"):
        self.name = name
        self._stats: dict[str, dict] = {}
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file  = LOGS_DIR / f"etl_{timestamp}.log"

        logger = logging.getLogger(self.name)
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()

        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Handler fichier (DEBUG+)
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)

        # Handler console (INFO+)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)

        logger.addHandler(fh)
        logger.addHandler(ch)

        logger.info(f"Logger initialisé → {log_file}")
        return logger

    # ── Raccourcis logging ────────────────────────────────────────────────────
    def info(self,    msg: str) -> None: self.logger.info(msg)
    def debug(self,   msg: str) -> None: self.logger.debug(msg)
    def warning(self, msg: str) -> None: self.logger.warning(msg)
    def error(self,   msg: str) -> None: self.logger.error(msg)

    # ── Logging des transformations avec stats ────────────────────────────────
    def log_transform(
        self,
        rule: str,
        before: int,
        after: int,
        entity: str = "",
        extra: str = "",
    ) -> None:
        """
        Logue une règle de transformation avec le delta de lignes.
        Accumule les stats pour le rapport final.
        """
        supprimees = before - after
        pct = (supprimees / before * 100) if before else 0
        msg = (
            f"[TRANSFORM] {rule:<30} | {entity:<15} | "
            f"{before:>6} → {after:>6} lignes "
            f"({supprimees:+d}, {pct:.1f}%)"
        )
        if extra:
            msg += f" | {extra}"
        self.logger.info(msg)

        # Accumulation pour rapport
        self._stats[rule] = {
            "entity":     entity,
            "avant":      before,
            "apres":      after,
            "supprimees": supprimees,
            "pct":        round(pct, 2),
            "extra":      extra,
        }

    def log_replace(self, rule: str, entity: str, nb: int, detail: str = "") -> None:
        """Logue une règle de remplacement (pas de suppression de lignes)."""
        msg = f"[TRANSFORM] {rule:<30} | {entity:<15} | {nb} valeurs remplacées"
        if detail:
            msg += f" ({detail})"
        self.logger.info(msg)
        self._stats[rule] = {
            "entity": entity, "avant": nb, "apres": nb,
            "supprimees": 0, "pct": 0.0, "extra": detail,
        }

    def log_extract(self, source: str, nb_lignes: int, fichier: str = "") -> None:
        self.logger.info(
            f"[EXTRACT]   {source:<30} | {nb_lignes:>6} lignes extraites"
            + (f" depuis {fichier}" if fichier else "")
        )

    def log_load(self, table: str, nb_lignes: int, strategie: str = "replace") -> None:
        self.logger.info(
            f"[LOAD]      {table:<30} | {nb_lignes:>6} lignes chargées [{strategie}]"
        )

    # ── Rapport de transformation ─────────────────────────────────────────────
    def generer_rapport(self, path: str | None = None) -> str:
        """Génère le rapport Markdown des transformations."""
        lines = [
            "# Rapport des transformations ETL — Mexora Analytics",
            "",
            f"Généré le : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "",
            "## Récapitulatif des règles appliquées",
            "",
            "| Règle | Entité | Avant | Après | Supprimées | % |",
            "|-------|--------|------:|-----:|----------:|---:|",
        ]
        for rule, s in self._stats.items():
            lines.append(
                f"| {rule} | {s['entity']} | {s['avant']} | {s['apres']} "
                f"| {s['supprimees']} | {s['pct']}% |"
            )

        rapport = "\n".join(lines)

        if path:
            Path(path).write_text(rapport, encoding="utf-8")
            self.logger.info(f"Rapport de transformation → {path}")

        return rapport

    def section(self, titre: str) -> None:
        """Affiche un séparateur de section dans les logs."""
        self.logger.info("=" * 70)
        self.logger.info(f"  {titre}")
        self.logger.info("=" * 70)


# Instance globale réutilisable
etl_logger = ETLLogger()

# src/config.py
import os
from dataclasses import dataclass
from dotenv import load_dotenv

# load_dotenv() lit le fichier .env et charge chaque ligne
# comme variable d'environnement du processus Python.
# Si la variable existe déjà (cas Docker en prod),
# elle ne l'écrase pas — comportement intentionnel.
load_dotenv()


def _require_env(key: str) -> str:
    """
    Lit une variable d'environnement obligatoire.

    Pourquoi cette fonction existe :
    os.getenv("X") retourne None silencieusement si X est absent.
    Ton programme continue et plante 50 lignes plus loin avec
    "AttributeError: 'NoneType' object has no attribute 'split'"
    — impossible à débugger.

    Avec _require_env, le programme plante IMMÉDIATEMENT au
    démarrage avec un message clair : quelle variable manque
    et comment la corriger.
    """
    value = os.getenv(key)
    if value is None:
        raise EnvironmentError(
            f"Variable d'environnement manquante : '{key}'\n"
            f"Vérifie ton fichier .env"
        )
    return value


@dataclass(frozen=True)
class DatabaseConfig:
    """
    Configuration PostgreSQL.

    @dataclass génère automatiquement __init__, __repr__, __eq__.
    frozen=True rend l'objet immuable — personne ne peut
    accidentellement modifier la config après sa création.
    """
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def connection_string(self) -> str:
        """
        Construit la chaîne de connexion SQLAlchemy.

        @property = s'utilise comme un attribut, pas une méthode.
        config.db.connection_string  (pas de parenthèses)
        """
        return (
            f"postgresql+psycopg2://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


@dataclass(frozen=True)
class APIConfig:
    """Configuration de l'API Open Food Facts."""
    base_url: str
    timeout: int
    page_size: int = 100
    max_pages: int = 5


@dataclass(frozen=True)
class AppConfig:
    """Configuration globale — regroupe toutes les sous-configs."""
    db: DatabaseConfig
    api: APIConfig


def load_config() -> AppConfig:
    """
    Charge et valide toute la configuration au démarrage.

    Toutes les variables obligatoires sont vérifiées ici.
    Si une manque, le programme plante immédiatement avec
    un message clair — avant d'avoir fait quoi que ce soit.
    """
    return AppConfig(
        db=DatabaseConfig(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=_require_env("POSTGRES_DB"),
            user=_require_env("POSTGRES_USER"),
            password=_require_env("POSTGRES_PASSWORD"),
        ),
        api=APIConfig(
            base_url=os.getenv(
                "OFF_API_BASE_URL",
                "https://world.openfoodfacts.org/cgi/search.pl"
            ),
            timeout=int(os.getenv("OFF_REQUEST_TIMEOUT", "30")),
        ),
    )


# Instance globale créée une fois au démarrage.
# Les autres modules font simplement :
# from src.config import config
# print(config.db.database)
config = load_config()
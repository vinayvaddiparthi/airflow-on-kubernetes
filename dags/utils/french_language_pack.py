from transliterate.base import TranslitLanguagePack


class FrenchLanguagePack(TranslitLanguagePack):
    language_code = "fr"
    language_name = "French"
    mapping = ("", "")
    reversed_specific_mapping = (
        "ÀàÂâÉéÈèÊêËëÎîÏïÔôÛûÙùÜüÇç",
        "AaAaEeEeEeEeIiIiOoUuUuUuCc",
    )
    reversed_specific_pre_processor_mapping = {"Œ": "Oe", "œ": "oe"}

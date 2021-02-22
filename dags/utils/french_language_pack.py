from transliterate.base import TranslitLanguagePack


class FrenchLanguagePack(TranslitLanguagePack):
    language_code = "fr"
    language_name = "French"
    mapping = (u"", u"")
    reversed_specific_mapping = (
        u"ÀàÂâÉéÈèÊêËëÎîÏïÔôÛûÙùÜüÇç",
        u"AaAaEeEeEeEeIiIiOoUuUuUuCc",
    )
    reversed_specific_pre_processor_mapping = {u"Œ": u"Oe", u"œ": u"oe"}

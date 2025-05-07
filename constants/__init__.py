# Todo: Once we upgrade to Django 3, use TextChoices(?)
# we have this pattern used in a couple places, it is for choice fields.
# intended for use only on classes with string values and nothing else.
class DjangoDropdown:
    
    @classmethod
    def choices(cls) -> list[tuple[str, str]]:
        # this forces a dropdown to have specific choices of the same name as the attribute itself
        # with underscores -> spaces and title casing.
        return [(choice, choice.replace("_", " ").title()) for choice in cls.values()]
    
    @classmethod
    def values(cls) -> list[str]:
        # get the string VALUES of the attributes of the class for every attribute that does not start with an underscore
        ret = []
        for property_name, property_value in vars(cls).items():
            if not property_name.startswith("_") and isinstance(property_value, str):
                ret.append(property_value)
        return ret

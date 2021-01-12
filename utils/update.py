def updateAttr(obj, dict):
        for key, value in dict.items():
            if hasattr(obj, key):
                setattr(obj, key, value)
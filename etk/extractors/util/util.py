def tf_transfer(x):
    if isinstance(x, str):
        if x.lower() == "true":
            return True
        if x.lower() == "false":
            return False
        return True
    else:
        return x
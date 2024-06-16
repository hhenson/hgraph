try:
    import perspective

    assert tuple(map(int, perspective.__version__.split("."))) >= (2, 8, 0)

    from ._perspective import *
    from ._perspetive_publish import *

except:
    pass

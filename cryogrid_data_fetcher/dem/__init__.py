from . import derived

for func_name in derived._TERRAIN_FUNCS:
    locals()[func_name] = getattr(derived, func_name)
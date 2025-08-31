def reference_data_loader(nomen):
    if nomen.lower()=='bcn':
        from reference_data.bcn import get_all_bcn
        return get_all_bcn()
    if nomen.lower().startswith('paysage'):
        from reference_data.paysage import get_paysage
        return get_paysage(nomen)
    if nomen.lower()=='google_sheet':
        from reference_data.google_sheet import get_all_correctifs
        return get_all_correctifs()
    

def lookup_table(ref, table:str):
    return ref[table]
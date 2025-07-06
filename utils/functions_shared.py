def get_sources(annee):
    # selection des sources existantes en fonction des années
    assert(annee >= 2004)
    sources = ['inscri', 'inge', 'priv']
    #sources = ['inge', 'priv']
    if 2004 <= annee <= 2007:
        sources.append('iufm')
    if annee > 2004:
        sources.append('ens')
    if annee > 2005:
        sources.append('mana')
    if annee > 2015:
        sources.append('enq26bis')
    if annee > 2016:
        sources.append('culture')
    return sources
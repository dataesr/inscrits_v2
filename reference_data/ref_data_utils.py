from reference_data.bcn import get_all_bcn
from reference_data.google_sheet import get_all_correctifs
from reference_data.paysage import get_paysage

CORRECTIFS_dict = get_all_correctifs()
PAYSAGE_dict = get_paysage('paysage')
BCN = get_all_bcn()
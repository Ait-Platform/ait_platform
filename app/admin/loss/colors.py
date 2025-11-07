# app/admin/loss/colors.py
#from .phase_item import band_label_for_pct

from app.admin.loss.phase_item import band_label_for_pct


BAR_CLASS = {...}  # same as above
CHIP_CLASS = {...}

def color_classes_for_map(phase_no, pct):
    band = band_label_for_pct(pct)
    return BAR_CLASS[int(phase_no)][band], CHIP_CLASS[int(phase_no)][band]

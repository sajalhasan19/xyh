# coding: utf8

"""
collection of useful function for producers
taken from:
    https://github.com/uhh-cms/topsf/blob/master/topsf/production/util.py
"""
from functools import partial

from columnflow.util import maybe_import

ak = maybe_import("awkward")
np = maybe_import("numpy")
coffea = maybe_import("coffea")

def ak_extract_fields(arr, fields, **kwargs):
    """
    Build an array containing only certain `fields` of an input array `arr`,
    preserving behaviors.
    """
    # reattach behavior
    if "behavior" not in kwargs:
        kwargs["behavior"] = arr.behavior

    return ak.zip(
        {
            field: getattr(arr, field)
            for field in fields
        },
        **kwargs,
    )

_lv_base = partial(ak_extract_fields, behavior=coffea.nanoevents.methods.nanoaod.behavior)

lv_xyzt =  partial(_lv_base, fields=["x", "y", "z", "t"], with_name="LorentzVector")

lv_mass = partial(_lv_base, fields=["pt", "eta", "phi", "mass"], with_name="PtEtaPhiMLorentzVector")

lv_energy = partial(_lv_base, fields=["pt", "eta", "phi", "energy"], with_name="PtEtaPhiELorentzVector")

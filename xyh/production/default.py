# coding: utf-8

"""
Column production methods related to higher-level features.
"""

import functools

from columnflow.production import Producer, producer
from columnflow.production.categories import category_ids
from columnflow.production.normalization import normalization_weights
from columnflow.production.cms.pileup import pu_weight
from columnflow.production.cms.btag import btag_weights
from columnflow.production.cms.electron import electron_weights, electron_mid_weights, electron_id_weights
from columnflow.production.cms.muon import muon_id_weights, muon_iso_weights
from columnflow.production.cms.top_pt_weight import top_pt_weight
from columnflow.production.cms.pdf import pdf_weights
from columnflow.production.cms.scale import murmuf_weights, murmuf_envelope_weights
from columnflow.util import maybe_import
from columnflow.columnar_util import EMPTY_FLOAT, set_ak_column, has_ak_column

from xyh.production.leptons import leading_lepton
from xyh.production.leptons import solve_neutrino_pz
from xyh.production.prepare_objects import prepare_objects
from xyh.production.utils import lv_mass
from xyh.selection.jet_selection import jet_selection


#from xyh.production.utils import compute_energy
# TODO: Add weight producer, i.e. SFs and all

ak = maybe_import("awkward")
coffea = maybe_import("coffea")
np = maybe_import("numpy")
maybe_import("coffea.nanoevents.methods.nanoaod")

set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)

@producer(
  uses={
    category_ids, normalization_weights, pu_weight, btag_weights,
    electron_weights, electron_mid_weights, electron_id_weights,
    muon_id_weights, muon_iso_weights,
    top_pt_weight, pdf_weights, murmuf_weights, murmuf_envelope_weights,
    prepare_objects, leading_lepton,
    solve_neutrino_pz, jet_selection,
    "Jet.{pt,eta,phi,mass,rawFactor,btagDeepFlavB}",
    "Bjet.{pt,eta,phi,mass}", #sh
    "MET.{pt,phi}",
    "process_id",
    "Lightjet",
  },
  produces={
    category_ids, normalization_weights, pu_weight, btag_weights,
    electron_weights, electron_mid_weights, electron_id_weights,
    muon_id_weights, muon_iso_weights,
    top_pt_weight, pdf_weights, murmuf_weights, murmuf_envelope_weights,
    prepare_objects, leading_lepton,
    solve_neutrino_pz,
    "event_number", "process_id",
    "mlnu", "mlnu_real", "mtlnu",
    "wboson.{pt,eta,phi,mass}",
    #"top_mass_manual",
    #"lv_bjet", #sh
    "lv_bjets",
    "m_bb",
    "m_H",
    "lv_bb",
    # "m_lead_b",
    # "lead_b_pt",
    "top_mass", #sh
    "top_pt",
    "lv_top", #sh
    "lv_lightjets", "lightjets_mass", "lightjets_pt",
    "lightjets_eta", "lightjets_phi",
    "lv_whad", "whad_mass",
    "lv_top_had", "top_had_mass", "top_had_pt",
    "lv_tt_bar", "tt_bar_mass", "tt_bar_pt",
    "Lightjet",
    "deltaR_qq",
    "deltaR_bb",
    "deltaR_jj",
    "m_X",
  },
  #exposed=True
)

def default(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
  # Build categories

  events = self[category_ids](events, **kwargs)
  #print("after category ids", type(events), events.fields)

  if self.dataset_inst.is_mc:
    events = self[normalization_weights](events, **kwargs)
    events = self[pu_weight](events, **kwargs)
    events = self[btag_weights](events, **kwargs)
    events = self[electron_weights](events, electron_mask=(events.Electron.pt >= 75), **kwargs)
    events = self[electron_mid_weights](events, electron_mask=(events.Electron.pt < 75), **kwargs)
    events = self[electron_id_weights](events, **kwargs)
    events = self[muon_id_weights](events, **kwargs)
    events = self[muon_iso_weights](events, **kwargs)
    # Top-pt reweighting is only defined for ttbar-like samples.
    if self.dataset_inst.x("is_ttbar", False):
      events = self[top_pt_weight](events, **kwargs)
    else:
      ones = np.ones(len(events), dtype=np.float32)
      events = set_ak_column_f32(events, "top_pt_weight", ones)
      events = set_ak_column_f32(events, "top_pt_weight_up", ones)
      events = set_ak_column_f32(events, "top_pt_weight_down", ones)
    if has_ak_column(events, "LHEPdfWeight"):
      events = self[pdf_weights](events, **kwargs)
    else:
      ones = np.ones(len(events), dtype=np.float32)
      events = set_ak_column_f32(events, "pdf_weight", ones)
      events = set_ak_column_f32(events, "pdf_weight_up", ones)
      events = set_ak_column_f32(events, "pdf_weight_down", ones)

    if has_ak_column(events, "LHEScaleWeight"):
      events = self[murmuf_weights](events, **kwargs)
      events = self[murmuf_envelope_weights](events, **kwargs)
    else:
      ones = np.ones(len(events), dtype=np.float32)
      for name in (
        "mur_weight", "mur_weight_up", "mur_weight_down",
        "muf_weight", "muf_weight_up", "muf_weight_down",
        "murmuf_envelope_weight", "murmuf_envelope_weight_up", "murmuf_envelope_weight_down",
      ):
        events = set_ak_column_f32(events, name, ones)

  events = self[leading_lepton](events, **kwargs)
  
  events = self[prepare_objects](events, **kwargs)

  events = self[solve_neutrino_pz](events, **kwargs)
  
  events = set_ak_column(events, "event_number", events.event)

  

  
  
  
  
  
  # Wlnu events
  wlnu = events.Neutrino.like(events.Leptons[:,0]).add(events.Leptons[:,0])
  delta_phi = events.Leptons[:,0].phi - events.MET.phi
  wlnu_mt = np.sqrt(2 * events.Leptons[:,0].pt * events.MET.pt * (1 - np.cos(delta_phi))) #sh
  #wlnu_mt = np.sqrt(wlnu.energy**2 - wlnu.pz**2)

  events = set_ak_column_f32(events, "mlnu", wlnu.mass)
  mlnu_real = ak.where(events.nu_has_real, wlnu.mass, EMPTY_FLOAT)
  events = set_ak_column_f32(events, "mlnu_real", mlnu_real)
  events = set_ak_column_f32(events, "mtlnu", wlnu_mt)

  # Now save the whole 4-momentum of the W
  lnu = ak.with_name(wlnu, "PtEtaPhiMLorentzVector")
  lnu = lv_mass(lnu)
  events = set_ak_column_f32(events, "wboson", lnu)

  #print("lnu_vector = ", events.wboson)
  # Interactive debugger
  # from IPython import embed; embed()
 
 #Manual method of Transverse mass reconstruction
  # E_w = compute_energy(events.wboson.pt, events.wboson.eta, events.wboson.mass)
  # E_b = compute_energy(events.Bjet.pt, events.Bjet.eta, events.Bjet.mass)
  
  # px_w = events.wboson.pt * np.cos(events.wboson.phi)
  # py_w = events.wboson.pt * np.sin(events.wboson.phi)
  # pz_w = events.wboson.pt * np.sinh(events.wboson.eta)

  # px_b = events.Bjet.pt * np.cos(events.Bjet.phi)
  # py_b = events.Bjet.pt * np.sin(events.Bjet.phi)
  # pz_b = events.Bjet.pt * np.sinh(events.Bjet.eta)
  
  # px_tot = px_w + px_b
  # py_tot = py_w + py_b
  # pz_tot = pz_w + pz_b
  # E_tot  = E_w + E_b
  
  # top_mass = np.sqrt(E_tot**2 - px_tot**2 - py_tot**2 - pz_tot**2)
  
  # events = set_ak_column_f32(events, "top_mass_manual", top_mass)

  # require at least 2 jets
  has2j = ak.num(events.Jet) >= 2

  # compute ΔR for leading pair, else set NaN
  deltaR_jj = ak.where(
      has2j,
      events.Jet[:, 0].delta_r(events.Jet[:, 1]),
      np.nan,
  )

  # store as float32 column
  events = set_ak_column_f32(events, "deltaR_jj", deltaR_jj)

  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  #from IPython import embed; embed()


  #sh
  # Define Lorentz vector for leading b-jet

  # has_bjet = ak.num(events.Bjet) > 0
  # print("has_bjet = ", has_bjet)


  # bjets = ak.mask(events.Bjet, has_bjet)  # or another b-jet selection
  
  # Build Lorentz vectors for all b-jets
  bjets_all = events.Bjet
  bjets_vec = ak.with_name(bjets_all, "PtEtaPhiMLorentzVector")
  bjets_vec = lv_mass(bjets_vec)

  # Save all b-jet Lorentz vectors
  events = set_ak_column_f32(events, "lv_bjets", bjets_vec)

  #print("type of lv_bjets = ", ak.type(events.lv_bjets))
  #print("lv_bjets = ", ak.num(events.lv_bjets, axis=1)[:20])





  # # Make sure lv_bjets and lv_w are ak Arrays with "PtEtaPhiMLorentzVector" behavior
  # # lv_bjets: shape (n_events, n_bjets_per_event)
  # # lv_w: shape (n_events,) or (n_events, n_w_per_event)

  # # Step 1: Broadcast bjets and W bosons
  wlnu_b_broadcast = ak.cartesian({"w": events.wboson, "b": events.lv_bjets}, axis=1)

  # # Step 2: Compute ΔR between each W and b-jet in event
  delta_r = wlnu_b_broadcast.w.delta_r(wlnu_b_broadcast.b)

  # # Step 3: Find the index of the b-jet with minimum ΔR for each W
  closest_bjet_wlnu_idx = ak.argmin(delta_r, axis=1, keepdims=True)

  # # Step 4: Select the closest b-jet per W boson
  closest_bjets_wlnu = ak.firsts(wlnu_b_broadcast.b[closest_bjet_wlnu_idx])


















  # creating m(bb) object

  bjets_padded = ak.pad_none(bjets_vec, 2, axis=-1)

  dibjets_vec = ak.with_name(bjets_padded, "PtEtaPhiMLorentzVector")
  dibjets_vec = lv_mass(dibjets_vec)

  #print("dibjets_vec = ", dibjets_vec)

  lv_bb = dibjets_vec[:,0] + dibjets_vec[:,1]
  lv_bb = ak.with_name(lv_bb, "PtEtaPhiMLorentzVector")
  lv_bb = lv_mass(lv_bb)

  events = set_ak_column_f32(events, "m_bb", ak.fill_none(lv_bb.mass, EMPTY_FLOAT))
  events = set_ak_column(events, "lv_bb", lv_bb)

  #print("m_bb = ", events.m_bb)


  # from IPython import embed; embed()

  # compute ΔR between the leading two b-jets when available
  bb_pairs = ak.combinations(bjets_vec, 2, fields=["b1", "b2"])
  deltaR_bb = ak.fill_none(
    ak.firsts(bb_pairs.b1.delta_r(bb_pairs.b2)),
    EMPTY_FLOAT,
  )
  events = set_ak_column_f32(events, "deltaR_bb", deltaR_bb)

  # from IPython import embed; embed()

  has2b = ak.num(bjets_vec, axis=1) >= 2
  valid_dr_bb = deltaR_bb != EMPTY_FLOAT
  m_H_mass = ak.fill_none(lv_bb.mass, EMPTY_FLOAT)
  m_H = ak.where(has2b & valid_dr_bb & (deltaR_bb <= 1.4), m_H_mass, EMPTY_FLOAT)
  events = set_ak_column_f32(events, "m_H", m_H)

  print("m_H = ", events.m_H)

  #from IPython import embed; embed()

   
  #Define Lorentz Vector for top 
  tWb = events.wboson.add(closest_bjets_wlnu)

  #print(" closest bjets to wlnu = ", closest_bjets_wlnu)

  #print("wboson = ", events.wboson)
  
  #print("tWb = ", tWb)
  
  lv_top = ak.with_name(tWb, "PtEtaPhiMLorentzVector")

  lv_top = lv_mass(lv_top)

  events = set_ak_column(events, "lv_top", lv_top)

  #print("top_vector = ", events.lv_top)

  #set top_mass and top_pt
  events = set_ak_column_f32(events, "top_mass", ak.fill_none(lv_top.mass, EMPTY_FLOAT))
  events = set_ak_column_f32(events, "top_pt", ak.fill_none(lv_top.pt, EMPTY_FLOAT))

  #print("top mass = ", events.top_mass)
  #print("top_pt = ",  events.top_pt)

  
  
  
  
  
  
  
  
  
  
  #Lorentz Vector for lightjets
  
  lv_lightjets = ak.with_name(events.Lightjet, "PtEtaPhiMLorentzVector")

  lv_lightjets = lv_mass(lv_lightjets)

  events = set_ak_column_f32(events, "lv_lightjets", lv_lightjets)
  events = set_ak_column_f32(events, "lightjets_mass", lv_lightjets.mass)
  events = set_ak_column_f32(events, "lightjets_pt", lv_lightjets.pt)
  events = set_ak_column_f32(events, "lightjets_eta", lv_lightjets.eta)
  events = set_ak_column_f32(events, "lightjets_phi", lv_lightjets.phi)

  # print("leading lightjet mass = ", events.lightjets_mass[:,0])
  # print("lightjet pt = ", events.lightjets_pt)

  # print("Final event fields:", events.fields)

  
  
  
  
  # # require at least 2 lightjets
  # has2q = ak.num(events.lv_lightjets) >= 2

  # # compute ΔR for leading pair, else set NaN
  # deltaR_qq_lead = ak.where(
  #     has2q,
  #     events.lv_lightjets[:, 0].delta_r(events.lv_lightjets[:, 1]),
  #     np.nan,
  # )

  # # store as float32 column
  # events = set_ak_column_f32(events, "deltaR_qq_lead", deltaR_qq_lead)
  
  # from IPython import embed; embed()



  

  #Now we have lv_lightjets, build W mass by selecting the two  lightjets.

  # choose the light-jet pair with minimum ΔR
  qq_pairs = ak.combinations(events.lv_lightjets, 2, fields=["q1", "q2"])
  delta_r_qq = qq_pairs.q1.delta_r(qq_pairs.q2)
  closest_pair_idx = ak.argmin(delta_r_qq, axis=1, keepdims=True)

  closest_q1 = ak.firsts(qq_pairs.q1[closest_pair_idx])
  closest_q2 = ak.firsts(qq_pairs.q2[closest_pair_idx])
  min_delta_r_qq = ak.fill_none(ak.firsts(delta_r_qq[closest_pair_idx]), np.nan)
  events = set_ak_column_f32(events, "deltaR_qq", min_delta_r_qq)

  whad = closest_q1.add(closest_q2)
  lv_whad = ak.with_name(whad, "PtEtaPhiMLorentzVector")
  lv_whad = lv_mass(lv_whad)

  events = set_ak_column_f32(events, "lv_whad", lv_whad)
  events = set_ak_column_f32(events, "whad_pt", ak.fill_none(lv_whad.pt, EMPTY_FLOAT))
  events = set_ak_column_f32(events, "whad_eta", ak.fill_none(lv_whad.eta, EMPTY_FLOAT))
  events = set_ak_column_f32(events, "whad_phi", ak.fill_none(lv_whad.phi, EMPTY_FLOAT))
  events = set_ak_column_f32(events, "whad_mass", ak.fill_none(lv_whad.mass, EMPTY_FLOAT)) 
  
  #print("lv_whad = ", events.lv_whad)



  # Construct hadronic W for the signal. Select two lightjets with minimum ΔR

  # Step 1: Cartesian product of all lightjets
  # qq_broadcast = ak.combinations(events.lv_lightjets, 2, fields=["q1", "q2"])
  # delta_r_qq = qq_broadcast.q1.delta_r(qq_broadcast.q2)
  # closest_idx = ak.argmin(delta_r_qq, axis=1)
  # closest_q1 = qq_broadcast.q1[closest_idx]
  # closest_q2 = qq_broadcast.q2[closest_idx]

  # print("closest_q1 = ", closest_q1)
  # print("closest_q2 = ", closest_q2)
  
  # lv_whad = closest_q1.add(closest_q2)

  # events = set_ak_column_f32(events, "lv_whad", lv_whad)


  # Make sure lv_bjets and lv_whad are ak Arrays with "PtEtaPhiMLorentzVector" behavior
  # lv_bjets: shape (n_events, n_bjets_per_event)
  # lv_whad: shape (n_events,) or (n_events, n_w_per_event)



  # exclude the leptonic b-jet from the hadronic candidate list
  bjet_local_idx = ak.local_index(events.lv_bjets, axis=1)
  lep_bjet_idx = ak.fill_none(ak.firsts(closest_bjet_wlnu_idx), -1)
  hadronic_mask = bjet_local_idx != lep_bjet_idx[..., None]
  hadronic_bjets = events.lv_bjets[hadronic_mask]

  whad_b_broadcast = ak.cartesian({"w": lv_whad, "b": hadronic_bjets}, axis=1)
  delta_r_whad = whad_b_broadcast.w.delta_r(whad_b_broadcast.b)
  closest_bjet_whad_idx = ak.argmin(delta_r_whad, axis=1, keepdims=True)
  closest_bjets_whad = ak.firsts(whad_b_broadcast.b[closest_bjet_whad_idx])

  #print("closest_bjets_whad = ", closest_bjets_whad)


  
  # Hadronic top reconstruction
  
  top_had = closest_bjets_whad + events.lv_whad

  #from IPython import embed; embed()
  
  #print("top_had = ", top_had)
  
  lv_top_had = ak.with_name(top_had, "PtEtaPhiMLorentzVector")

  lv_top_had = lv_mass(lv_top_had)

  events = set_ak_column_f32(events, "lv_top_had", lv_top_had)

  #print("top_had vector = ", events.lv_top_had)
  #print(ak.type(lv_top_had))
  #print(ak.num(lv_top_had, axis=0))


  
  
  #set top_had_mass and top_had_pt
  events = set_ak_column_f32(events, "top_had_mass", ak.fill_none(lv_top_had.mass, EMPTY_FLOAT))
  events = set_ak_column_f32(events, "top_had_pt", ak.fill_none(lv_top_had.pt, EMPTY_FLOAT))

  #print("top mass = ", events.top_had_mass)
  #print("top_pt = ",  events.top_had_pt)

  #print("lv_top = ", lv_top)
  #print("lv_top_had = ", lv_top_had)

  
  
  
  
  # reconstruction for tt-bar

  ttbar = lv_top + lv_top_had
  
  #from IPython import embed; embed()
  
  lv_tt_bar = ak.with_name(ttbar, "PtEtaPhiMLorentzVector")

  lv_tt_bar = lv_mass(lv_tt_bar)

  events = set_ak_column_f32(events, "lv_tt_bar", lv_tt_bar)

  #print("tt_bar vector = ", events.lv_tt_bar)

  # set top_had_mass and top_had_pt
  events = set_ak_column_f32(events, "tt_bar_mass", ak.fill_none(lv_tt_bar.mass, EMPTY_FLOAT))
  events = set_ak_column_f32(events, "tt_bar_pt", ak.fill_none(lv_tt_bar.pt, EMPTY_FLOAT))

  #print("tt_bar mass = ", events.tt_bar_mass)
  #print("tt_bar pt = ",  events.tt_bar_pt)

  # from IPython import embed; embed()

  #Construct X_mass
  valid_m_tt = events.tt_bar_mass != EMPTY_FLOAT
  valid_m_bb = events.m_bb != EMPTY_FLOAT
  valid = valid_m_tt & valid_m_bb

  m_tt_clean = ak.where(valid, events.tt_bar_mass, EMPTY_FLOAT)
  m_bb_clean = ak.where(valid, events.m_bb, EMPTY_FLOAT)

  m_X = ak.where(valid, m_tt_clean + m_bb_clean, EMPTY_FLOAT)
  events = set_ak_column_f32(events, "m_X", m_X)
 

  #from IPython import embed; embed()

  return events

# @default.init
# def default_init(self: Producer) -> None:
#   # add_categories_bjets(self.config_inst)
#   add_categories_njets(self.config_inst)

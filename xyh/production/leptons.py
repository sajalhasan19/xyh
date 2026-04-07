# coding: utf-8

"""
Column producers related to leptons.
"""
from columnflow.production import Producer, producer
from columnflow.util import maybe_import
from columnflow.columnar_util import set_ak_column

ak = maybe_import("awkward")
np = maybe_import("numpy")
coffea = maybe_import("coffea")
maybe_import("coffea.nanoevents.methods.nanoaod")
#set_ak_column_f32 = functools.partial(set_ak_column, value_type=np.float32)

@producer(
  uses={
    "category_ids",
    "Electron.{pt,eta,phi,mass,pdgId}",
    "Muon.{pt,eta,phi,mass,pdgId}"
  },
  produces={
    "Leptons.{pt,eta,phi,mass,pdgId}"
  },
)
def leading_lepton(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
  """
  Choose either muon or electron as the main lepton per event
  based on `channel_id` information and write it to a new column
  `Lepton`.
  """

  # extract only LV columns, take leading Ele and Mu in collection
  muon = events.Muon[["pt", "eta", "phi", "mass", "pdgId"]]
  electron = events.Electron[["pt", "eta", "phi", "mass", "pdgId"]]
  # Select only 1e or 1mu events
  # Since the selection step already keeps only these events, no need to filter based on cat_id
  leptons = ak.concatenate([muon, electron], axis=1)

  # attach lorentz vector behavior to lepton
  leptons = ak.with_name(leptons, "PtEtaPhiMLorentzVector")
  # commit lepton to events array
  events = set_ak_column(events, "Leptons", leptons)
  
  # lead_lepton = events.Leptons[:,0]
  
  # events = set_ak_column(events, "lead_leptons", lead_lepton)

  return events



@producer(
  uses={
    "category_ids",
    "Leptons.{pt,eta,phi,mass,pdgId}",
    "MET.{pt,phi}",
  },
  produces={
    "Neutrino.{pt,eta,phi,mass}",
    "nu_has_real",
  },
)

  #neutrino p_z construction
  
def solve_neutrino_pz(self: Producer, events: ak.Array, **kwargs) -> ak.Array:
  mW = 80.4  # GeV
  lead_lep = events.Leptons[:,0]
  
  lep_mass = lead_lep.mass
  lep_pt = lead_lep.pt
  lep_phi = lead_lep.phi
  lep_eta = lead_lep.eta
 
  lep_px = lep_pt * np.cos(lep_phi)
  lep_py = lep_pt * np.sin(lep_phi)
  lep_pz = lep_pt * np.sinh(lep_eta)
  lep_E  = np.sqrt(lep_px**2 + lep_py**2 + lep_pz**2 + lep_mass**2)
  
  met_px = events.MET.pt * np.cos(events.MET.phi)
  met_py = events.MET.pt * np.sin(events.MET.phi)
  
  mu = (mW**2) / 2 + lep_px * met_px + lep_py * met_py

  a = lep_E**2 - lep_pz**2
  print("a = ", a)
  b = -2 * mu * lep_pz
  print("b = ", b)
  c = lep_E**2 * (met_px**2 + met_py**2) - mu**2
  print("c = ", c)
  discriminant = b**2 - 4 * a * c
  
  # Default to real part only if discriminant is negative
  pz_nu = -b / (2 * a)
  
  print(len(pz_nu))

  has_real = discriminant >= 0
  events = set_ak_column(events, "nu_has_real", has_real)
  
  
  sqrt_disc = np.sqrt(discriminant)
  print("sqrt_disc= ", sqrt_disc)
  sol1 = (-b + sqrt_disc) / (2 * a)
  print("sol1 = ", sol1)
  sol2 = (-b - sqrt_disc) / (2 * a)
  print("sol2 = ", sol2)
  # Choose solution with smaller abs(pz)
  # This works only inside the has_real mask
  abs_sol1 = np.abs(sol1)
  abs_sol2 = np.abs(sol2)
  print("abs_sol1 = ", abs_sol1)
  print("abs_sol2 = ", abs_sol2)
  print("sol1 length = ", len(sol1))
  print("sol2 length = ", len(sol2))

  chosen_sol_full = ak.where(abs_sol1 < abs_sol2, sol1, sol2)
  
  print("chosen_sol_full length = ", len(chosen_sol_full))
  print("chosen_sol_full = ", chosen_sol_full)
  print("has_real = ", has_real)

  
  # # Initialize a full-length array
  # filled_chosen_sol = ak.full_like(pz_nu, 0.0)  # or np.nan
  # filled_chosen_sol = ak.where(has_real, chosen_sol, )
  # filled_chosen_sol = ak.where(has_real, chosen_sol, pz_nu)

  # Now fill pz_nu: for real solutions use chosen_sol, else fallback to default
  
  # pz_nu_filled = ak.full_like(has_real, 0.0)
  #chosen_sol = ak.where(has_real, chosen_sol, 0.0)
  
  print("chosen_sol length =", len(chosen_sol_full))
  print("chosen_sol = ", chosen_sol_full)
  
  pz_nu_real = ak.where(has_real, chosen_sol_full, pz_nu)
  
  print("pz_nu_real = ", pz_nu_real)
  
  print("pz_nu_real_length = ", len(pz_nu_real))
  
  
  nu_eta = np.arcsinh(pz_nu_real / events.MET.pt)
  print("nu_eta = ", nu_eta)
  
  #debugging
  
  # print("Shapes:")
  # print("pt:", ak.type(events.MET.pt))
  # print("eta:", ak.type(nu_eta))
  # print("phi:", ak.type(events.MET.phi))
  # print("mass:", ak.type(ak.zeros_like(events.MET.pt)))

  neutrino = ak.zip({
      "pt": events.MET.pt,
      "eta": nu_eta,
      "phi": events.MET.phi,
      "mass": ak.zeros_like(events.MET.pt),
  }, with_name="PtEtaPhiMLorentzVector")
  
  events = set_ak_column(events, "Neutrino", neutrino)
  
  print("Neutrino = ", events.Neutrino)

  return events

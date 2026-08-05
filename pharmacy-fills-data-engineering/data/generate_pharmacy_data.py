"""
Synthetic healthcare / pharmacy fills dataset generator.

Generates a set of related CSVs suitable for a medallion-architecture
data engineering demo (bronze = raw landed files):

  dim_patients.csv       - patient master data
  dim_prescribers.csv    - prescribing physicians (NPI, specialty)
  dim_pharmacies.csv     - dispensing pharmacies
  dim_plans.csv          - payer / health plan reference
  dim_drugs.csv          - drug / NDC reference (formulary-style)
  fact_fills_raw.csv     - raw pharmacy claim fills (messy, bronze-quality)
  streaming_pos_events.jsonl - sample of "real-time" POS/adjudication events

The fills file is DELIBERATELY imperfect (nulls, duplicate claims,
inconsistent casing/date formats, a few orphan foreign keys, occasional
negative day-supply typos) so it's useful for demonstrating silver-layer
cleansing logic (dedupe, standardize, validate, SCD handling).
"""

import csv
import json
import random
import uuid
from datetime import datetime, timedelta

import numpy as np
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)
np.random.seed(42)

N_PATIENTS = 600
N_PRESCRIBERS = 150
N_PHARMACIES = 45
N_PLANS = 8
N_FILLS = 30000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2025, 12, 31)

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

STATES = ["MA", "NH", "CT", "RI", "NY", "NJ", "PA", "ME", "VT", "CA", "TX", "FL"]

DRUG_CATALOG = [
    # (drug_name, generic_name, drug_class, is_opioid, is_controlled, schedule, form, common_strengths, awp_base)
    ("Lipitor", "atorvastatin", "Statin", False, False, None, "tablet", ["10mg", "20mg", "40mg", "80mg"], 45.0),
    ("Crestor", "rosuvastatin", "Statin", False, False, None, "tablet", ["5mg", "10mg", "20mg"], 60.0),
    ("Metformin", "metformin HCl", "Antidiabetic", False, False, None, "tablet", ["500mg", "850mg", "1000mg"], 12.0),
    ("Jardiance", "empagliflozin", "Antidiabetic", False, False, None, "tablet", ["10mg", "25mg"], 550.0),
    ("Ozempic", "semaglutide", "Antidiabetic/GLP-1", False, False, None, "injection", ["0.25mg", "0.5mg", "1mg"], 950.0),
    ("Lisinopril", "lisinopril", "ACE Inhibitor", False, False, None, "tablet", ["5mg", "10mg", "20mg", "40mg"], 10.0),
    ("Amlodipine", "amlodipine besylate", "Calcium Channel Blocker", False, False, None, "tablet", ["2.5mg", "5mg", "10mg"], 9.0),
    ("Metoprolol", "metoprolol tartrate", "Beta Blocker", False, False, None, "tablet", ["25mg", "50mg", "100mg"], 8.0),
    ("Levothyroxine", "levothyroxine sodium", "Thyroid", False, False, None, "tablet", ["25mcg", "50mcg", "75mcg", "100mcg"], 14.0),
    ("Amoxicillin", "amoxicillin", "Antibiotic", False, False, None, "capsule", ["250mg", "500mg"], 18.0),
    ("Azithromycin", "azithromycin", "Antibiotic", False, False, None, "tablet", ["250mg", "500mg"], 22.0),
    ("Ciprofloxacin", "ciprofloxacin", "Antibiotic", False, False, None, "tablet", ["250mg", "500mg"], 25.0),
    ("Albuterol", "albuterol sulfate", "Bronchodilator", False, False, None, "inhaler", ["90mcg"], 65.0),
    ("Fluticasone", "fluticasone propionate", "Corticosteroid", False, False, None, "inhaler", ["44mcg", "110mcg"], 70.0),
    ("Omeprazole", "omeprazole", "PPI", False, False, None, "capsule", ["20mg", "40mg"], 16.0),
    ("Sertraline", "sertraline HCl", "SSRI", False, False, None, "tablet", ["25mg", "50mg", "100mg"], 20.0),
    ("Escitalopram", "escitalopram oxalate", "SSRI", False, False, None, "tablet", ["5mg", "10mg", "20mg"], 24.0),
    ("Alprazolam", "alprazolam", "Benzodiazepine", False, True, "IV", "tablet", ["0.25mg", "0.5mg", "1mg"], 30.0),
    ("Lorazepam", "lorazepam", "Benzodiazepine", False, True, "IV", "tablet", ["0.5mg", "1mg", "2mg"], 28.0),
    ("Oxycodone", "oxycodone HCl", "Opioid", True, True, "II", "tablet", ["5mg", "10mg", "15mg", "20mg"], 40.0),
    ("Hydrocodone/APAP", "hydrocodone/acetaminophen", "Opioid", True, True, "II", "tablet", ["5/325mg", "10/325mg"], 35.0),
    ("Tramadol", "tramadol HCl", "Opioid", True, True, "IV", "tablet", ["50mg", "100mg"], 20.0),
    ("Fentanyl Patch", "fentanyl transdermal", "Opioid", True, True, "II", "patch", ["25mcg/hr", "50mcg/hr"], 210.0),
    ("Gabapentin", "gabapentin", "Anticonvulsant", False, False, None, "capsule", ["100mg", "300mg", "600mg"], 15.0),
    ("Warfarin", "warfarin sodium", "Anticoagulant", False, False, None, "tablet", ["2mg", "5mg"], 12.0),
    ("Eliquis", "apixaban", "Anticoagulant", False, False, None, "tablet", ["2.5mg", "5mg"], 560.0),
    ("Humira", "adalimumab", "Biologic/Immunomodulator", False, False, None, "injection", ["40mg"], 6800.0),
    ("Insulin Glargine", "insulin glargine", "Antidiabetic/Insulin", False, False, None, "injection", ["100units/mL"], 350.0),
    ("Prednisone", "prednisone", "Corticosteroid", False, False, None, "tablet", ["5mg", "10mg", "20mg"], 9.0),
    ("Ibuprofen", "ibuprofen", "NSAID", False, False, None, "tablet", ["200mg", "400mg", "600mg"], 7.0),
]

CHANNELS = ["retail", "mail", "specialty"]
CHANNEL_WEIGHTS = [0.72, 0.18, 0.10]

REJECT_CODES = [None, None, None, None, None, "70-PA_REQUIRED", "75-PRIOR_AUTH", "76-PLAN_LIMIT_EXCEEDED", "88-DUR_REJECT"]
REJECT_WEIGHTS = [0.90, 0.90, 0.90, 0.90, 0.90, 0.02, 0.02, 0.03, 0.03]
# normalize will happen implicitly via random.choices


def npi():
    return str(random.randint(1000000000, 1999999999))


def ndc(base_seed):
    # Fake but NDC-shaped: 5-4-2
    return f"{base_seed:05d}-{random.randint(1000,9999):04d}-{random.randint(10,99):02d}"


# ---------------------------------------------------------------------------
# Dimension: Plans
# ---------------------------------------------------------------------------
plan_rows = []
payers = ["Aetna", "UnitedHealthcare", "Cigna", "BlueCross BlueShield", "Humana", "Anthem", "Medicaid State Plan", "Medicare Part D"]
for i, payer in enumerate(payers[:N_PLANS], start=1):
    plan_rows.append({
        "plan_id": f"PLAN{i:03d}",
        "plan_name": f"{payer} {'Advantage' if 'Medicare' in payer else 'Standard'} Plan",
        "payer": payer,
        "plan_type": random.choice(["Commercial", "Medicare", "Medicaid"]) if "Medicaid" not in payer and "Medicare" not in payer else ("Medicaid" if "Medicaid" in payer else "Medicare"),
        "formulary_tier_count": random.choice([3, 4, 5]),
    })

# ---------------------------------------------------------------------------
# Dimension: Drugs
# ---------------------------------------------------------------------------
drug_rows = []
for i, d in enumerate(DRUG_CATALOG, start=1):
    name, generic, drug_class, is_opioid, is_controlled, schedule, form, strengths, awp_base = d
    for strength in strengths:
        drug_rows.append({
            "ndc": ndc(10000 + i),
            "drug_name": name,
            "generic_name": generic,
            "drug_class": drug_class,
            "strength": strength,
            "form": form,
            "is_opioid": is_opioid,
            "is_controlled_substance": is_controlled,
            "dea_schedule": schedule if schedule else "",
            "is_generic": random.choice([True, True, False]),
            "awp_unit_price": round(awp_base * random.uniform(0.85, 1.15), 2),
        })

# ---------------------------------------------------------------------------
# Dimension: Prescribers
# ---------------------------------------------------------------------------
specialties = ["Internal Medicine", "Family Medicine", "Cardiology", "Endocrinology",
               "Psychiatry", "Pain Management", "Pediatrics", "Orthopedics", "Oncology", "Nurse Practitioner"]
prescriber_rows = []
for i in range(1, N_PRESCRIBERS + 1):
    prescriber_rows.append({
        "prescriber_id": f"PRESC{i:05d}",
        "npi": npi(),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "specialty": random.choice(specialties),
        "state": random.choice(STATES),
    })

# ---------------------------------------------------------------------------
# Dimension: Pharmacies
# ---------------------------------------------------------------------------
chains = ["CVS Pharmacy", "Walgreens", "Rite Aid", "Costco Pharmacy", "Walmart Pharmacy",
          "Kroger Pharmacy", "Independent Pharmacy", "MailRx Home Delivery", "SpecialtyCare Rx"]
pharmacy_rows = []
for i in range(1, N_PHARMACIES + 1):
    chain = random.choice(chains)
    channel = "mail" if "Mail" in chain else ("specialty" if "Specialty" in chain else "retail")
    pharmacy_rows.append({
        "pharmacy_id": f"PHARM{i:04d}",
        "pharmacy_name": f"{chain} #{random.randint(100,9999)}" if channel == "retail" else chain,
        "npi": npi(),
        "channel": channel,
        "state": random.choice(STATES),
        "city": fake.city(),
    })

# ---------------------------------------------------------------------------
# Dimension: Patients
# ---------------------------------------------------------------------------
patient_rows = []
for i in range(1, N_PATIENTS + 1):
    dob = fake.date_of_birth(minimum_age=1, maximum_age=95)
    patient_rows.append({
        "patient_id": f"PAT{i:06d}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "date_of_birth": dob.isoformat(),
        "gender": random.choice(["F", "M"]),
        "state": random.choice(STATES),
        "plan_id": random.choice(plan_rows)["plan_id"],
    })

# assign each patient a chronic-condition profile so refill patterns look realistic
chronic_patients = random.sample(patient_rows, k=int(N_PATIENTS * 0.35))
chronic_ids = {p["patient_id"] for p in chronic_patients}

# ---------------------------------------------------------------------------
# Fact: Fills (intentionally messy for silver-layer cleansing exercises)
# ---------------------------------------------------------------------------


def random_date(start, end):
    delta = end - start
    return start + timedelta(days=random.randint(0, delta.days), seconds=random.randint(0, 86399))


def messy_date_format(dt):
    """Return the date in one of several inconsistent string formats."""
    fmt = random.choice(["%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y", "%Y-%m-%dT%H:%M:%S"])
    return dt.strftime(fmt)


fills_fieldnames = [
    "fill_id", "patient_id", "prescriber_id", "pharmacy_id", "plan_id", "ndc",
    "drug_name", "fill_date", "days_supply", "quantity_dispensed", "refill_number",
    "is_refill", "channel", "reject_code", "ingredient_cost", "dispensing_fee",
    "copay_amount", "plan_paid_amount", "total_paid_amount", "_source_system", "_ingested_at",
]

fill_rows = []
fill_counter = 1

for _ in range(N_FILLS):
    patient = random.choice(patient_rows)
    drug = random.choice(drug_rows)
    prescriber = random.choice(prescriber_rows)
    pharmacy = random.choice(pharmacy_rows)
    fill_date = random_date(START_DATE, END_DATE)

    is_chronic = patient["patient_id"] in chronic_ids
    days_supply = random.choice([30, 30, 30, 90]) if is_chronic else random.choice([7, 10, 14, 30])
    quantity = days_supply * random.choice([1, 1, 2])
    refill_number = random.randint(0, 11) if is_chronic else random.randint(0, 2)

    reject_code = random.choices(REJECT_CODES, weights=REJECT_WEIGHTS, k=1)[0]
    rejected = reject_code is not None

    ingredient_cost = round(drug["awp_unit_price"] * (quantity / 30.0) * random.uniform(0.9, 1.05), 2)
    dispensing_fee = round(random.uniform(1.5, 3.5), 2)
    total_cost = 0.0 if rejected else round(ingredient_cost + dispensing_fee, 2)
    copay = 0.0 if rejected else round(total_cost * random.uniform(0.05, 0.35), 2)
    plan_paid = 0.0 if rejected else round(total_cost - copay, 2)

    row = {
        "fill_id": f"FILL{fill_counter:07d}",
        "patient_id": patient["patient_id"],
        "prescriber_id": prescriber["prescriber_id"],
        "pharmacy_id": pharmacy["pharmacy_id"],
        "plan_id": patient["plan_id"],
        "ndc": drug["ndc"],
        # deliberately inconsistent casing to demo standardization
        "drug_name": random.choice([drug["drug_name"], drug["drug_name"].upper(), drug["drug_name"].lower()]),
        "fill_date": messy_date_format(fill_date),
        "days_supply": days_supply if random.random() > 0.003 else -days_supply,  # rare data-entry error
        "quantity_dispensed": quantity,
        "refill_number": refill_number,
        "is_refill": refill_number > 0,
        "channel": random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0],
        "reject_code": reject_code if reject_code else "",
        "ingredient_cost": ingredient_cost,
        "dispensing_fee": dispensing_fee,
        "copay_amount": copay,
        "plan_paid_amount": plan_paid,
        "total_paid_amount": total_cost,
        "_source_system": random.choice(["NCPDP_SWITCH", "PBM_FEED", "PHARMACY_POS"]),
        "_ingested_at": (fill_date + timedelta(hours=random.randint(1, 26))).isoformat(),
    }
    fill_rows.append(row)
    fill_counter += 1

# Inject some null values in a few non-key columns (bronze realism)
for _ in range(int(N_FILLS * 0.01)):
    row = random.choice(fill_rows)
    row[random.choice(["prescriber_id", "reject_code", "copay_amount"])] = ""

# Inject duplicate claims (same fill resubmitted) - common in raw PBM feeds
duplicate_sample = random.sample(fill_rows, k=int(N_FILLS * 0.008))
fill_rows.extend([dict(r) for r in duplicate_sample])

# Inject a few orphan foreign keys (patient/prescriber not in dimension) to demo referential checks
for _ in range(15):
    row = random.choice(fill_rows)
    row["prescriber_id"] = f"PRESC{random.randint(90000,99999)}"

random.shuffle(fill_rows)

# ---------------------------------------------------------------------------
# Streaming sample: real-time POS / adjudication events (JSONL)
# ---------------------------------------------------------------------------
stream_events = []
for row in random.sample(fill_rows, k=500):
    stream_events.append({
        "event_id": str(uuid.uuid4()),
        "event_type": "claim_adjudicated",
        "fill_id": row["fill_id"],
        "pharmacy_id": row["pharmacy_id"],
        "status": "REJECTED" if row["reject_code"] else "APPROVED",
        "event_timestamp": row["_ingested_at"],
    })
stream_events.sort(key=lambda e: e["event_timestamp"])

# ---------------------------------------------------------------------------
# Write files
# ---------------------------------------------------------------------------
OUT = "/home/claude/pharmacy_data"
import os
os.makedirs(OUT, exist_ok=True)


def write_csv(path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


write_csv(f"{OUT}/dim_plans.csv", plan_rows, list(plan_rows[0].keys()))
write_csv(f"{OUT}/dim_drugs.csv", drug_rows, list(drug_rows[0].keys()))
write_csv(f"{OUT}/dim_prescribers.csv", prescriber_rows, list(prescriber_rows[0].keys()))
write_csv(f"{OUT}/dim_pharmacies.csv", pharmacy_rows, list(pharmacy_rows[0].keys()))
write_csv(f"{OUT}/dim_patients.csv", patient_rows, list(patient_rows[0].keys()))
write_csv(f"{OUT}/fact_fills_raw.csv", fill_rows, fills_fieldnames)

with open(f"{OUT}/streaming_pos_events.jsonl", "w", encoding="utf-8") as f:
    for e in stream_events:
        f.write(json.dumps(e) + "\n")

print("Rows generated:")
print("  patients:", len(patient_rows))
print("  prescribers:", len(prescriber_rows))
print("  pharmacies:", len(pharmacy_rows))
print("  plans:", len(plan_rows))
print("  drugs:", len(drug_rows))
print("  fills (incl. duplicates):", len(fill_rows))
print("  streaming events:", len(stream_events))

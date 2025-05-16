import pandas as pd
import frappe
from hijridate import Hijri
from datetime import datetime

def safe_log_error(title, row):
    MAX_LENGTH = 140
    msg = f"Failed to insert row: {row}"
    if len(msg) > MAX_LENGTH:
        msg = msg[:MAX_LENGTH - 3] + "..."
    frappe.log_error(msg, title)

def convert_hijri_to_gregorian(hijri_date):
    try:
        if not hijri_date or pd.isna(hijri_date):
            return None

        # Handle both string formats and datetime-like inputs
        if isinstance(hijri_date, str):
            hijri_date = hijri_date.strip()
            if "-" in hijri_date:
                parts = hijri_date.split("-")
            elif "/" in hijri_date:
                parts = hijri_date.split("/")
            else:
                return None

            # Ensure we have exactly 3 parts (day, month, year or year, month, day)
            if len(parts) != 3:
                return None

            # Try parsing in both formats
            try:
                # Format 1: YYYY-MM-DD
                y, m, d = map(int, parts)
            except ValueError:
                # Format 2: DD-MM-YYYY
                d, m, y = map(int, parts)

        elif isinstance(hijri_date, datetime):
            return hijri_date.date()

        else:
            return None

        g_date = Hijri(y, m, d).to_gregorian()
        return datetime(g_date.year, g_date.month, g_date.day).date()

    except Exception as e:
        return None  # Return None if conversion fails

@frappe.whitelist()
def import_vehicle_data(file_url):
    file_doc = frappe.get_doc("File", {"file_url": file_url})
    file_path = frappe.utils.get_files_path(file_doc.file_name)

    df_raw = pd.read_excel(file_path, header=None)

    header_row_idx = None
    for idx, row in df_raw.iterrows():
        if "Plate Number" in row.values and "Chassis Number" in row.values:
            header_row_idx = idx
            break

    if header_row_idx is None:
        frappe.throw("Header row not found. Please upload a valid vehicle file.")

    df = pd.read_excel(file_path, header=header_row_idx)

    df = df.rename(columns={
        "Plate Number": "plate_number",
        "Plate Type": "plate_type",
        "Branch Name": "branch_name",
        "Vehicle Maker": "vehicle_maker",
        "Vehicle Model": "vehicle_model",
        "Model Year": "model_year",
        "Sequence Number": "sequence_number",
        "Chassis Number": "chassis_number",
        "Major Color": "major_color",
        "vehicle Status": "vehicle_status",
        "Ownership Date": "ownership_date",
        "License Expiry Date": "license_expiry_date",
        "Inspection Expiry Date": "inspection_expiry_date",
        "Actual Driver Id": "actual_driver_id",
        "Actual Driver Name": "actual_driver_name",
        "MVPI Status": "mvpi_status",
        "Insurance Status": "insurance_status",
        "Restriction Status": "restriction_status",
        "Istemarah issue Date": "istemarah_issue_date",
        "Vehicle Status": "vehicle_status2",
        "Body Type": "body_type"
    })

    # Replace hyphens and blanks with None
    df = df.replace("-", None)
    df = df.applymap(lambda x: None if pd.isna(x) or str(x).strip() == "" else x)

    # Convert Hijri dates to Gregorian for relevant fields
    hijri_date_fields = ["ownership_date", "license_expiry_date", "inspection_expiry_date", "istemarah_issue_date"]
    for field in hijri_date_fields:
        if field in df.columns:
            df[field] = df[field].apply(convert_hijri_to_gregorian)

    skipped_rows = []

    for row in df.to_dict(orient="records"):
        if not row.get("plate_number") or not row.get("chassis_number"):
            skipped_rows.append(row)
            continue

        if not frappe.db.exists("Vehicle Data", {"plate_number": row.get("plate_number")}):
            try:
                doc = frappe.get_doc({"doctype": "Vehicle Data", **row})
                doc.insert(ignore_permissions=True)
            except Exception as e:
                safe_log_error("Vehicle Data Import Error", row)

    if skipped_rows:
        frappe.log_error(f"{len(skipped_rows)} row(s) skipped due to missing mandatory fields.", "Vehicle Import Skipped Rows")

    return "✅ Vehicle Data Imported Successfully!"













import frappe
from frappe.custom.doctype.custom_field.custom_field import CustomField

@frappe.whitelist()
def transfer_to_vehicle():
    create_missing_vehicle_fields()

    vehicle_data_list = frappe.get_all("Vehicle Data", fields=["*"])
    transferred = 0
    skipped = []

    for data in vehicle_data_list:
        license_plate = data.get("plate_number")
        make = data.get("vehicle_maker")
        model = data.get("vehicle_model")
        last_odometer = data.get("last_odometer") or 0
        uom = data.get("uom") or "Kilometer"

        missing_fields = []
        if not license_plate:
            missing_fields.append("license_plate")
        if not make:
            missing_fields.append("make")
        if not model:
            missing_fields.append("model")

        if missing_fields:
            skipped.append(f"- Row: {data.get('plate_number') or data.get('name')}, Missing: {', '.join(missing_fields)}")
            continue

        try:
            vehicle = frappe.new_doc("Vehicle")
            vehicle.license_plate = license_plate
            vehicle.make = make
            vehicle.model = model
            vehicle.last_odometer = last_odometer
            vehicle.uom = uom

            # Standard / optional fields
            vehicle.fuel_type = data.get("fuel_type") or "Petrol"
            vehicle.color = data.get("major_color")
            vehicle.wheels = data.get("wheels") or 0
            vehicle.doors = data.get("doors") or 0
            vehicle.chassis_no = data.get("chassis_number") or "L2YPCKLCXR0L07082"
            vehicle.chassis_number = data.get("chassis_number")
            vehicle.engine_no = data.get("engine_no")
            vehicle.seating_capacity = data.get("seating_capacity")
            vehicle.owner_name = data.get("owner_name")
            vehicle.acquisition_date = data.get("acquisition_date")
            vehicle.vehicle_value = data.get("vehicle_value") or 0.00
            vehicle.location = data.get("location")
            vehicle.employee = data.get("employee")
            vehicle.insurance_company = data.get("insurance_company")
            vehicle.policy_no = data.get("policy_no")
            vehicle.start_date = data.get("insurance_start_date")
            vehicle.end_date = data.get("insurance_end_date")
            vehicle.last_carbon_check = data.get("last_carbon_check")
            vehicle.sequence_date = data.get("sequence_date")
            vehicle.license_expiry_date = data.get("license_expiry_date")
            vehicle.license_display_date = data.get("license_display_date")
            vehicle.vehicle_model = data.get("vehicle_model")
            vehicle.vehicle_maker = data.get("vehicle_maker")
            vehicle.plate_number = data.get("plate_number")
            vehicle.plate_type = data.get("plate_type")
            vehicle.branch_name = data.get("branch_name")
            

            # Dynamically added custom fields
            vehicle.actual_driver_id = data.get("actual_driver_id")
            vehicle.actual_driver_name = data.get("actual_driver_name")
            vehicle.mvpi_status = data.get("mvpi_status")
            vehicle.insurance_status = data.get("insurance_status")
            vehicle.restriction_status = data.get("restriction_status")
            vehicle.istemarah_issue_date = data.get("istemarah_issue_date")
            vehicle.vehicle_status = data.get("vehicle_status")
            vehicle.body_type = data.get("body_type")
            vehicle.inspection_expiry_date = data.get("inspection_expiry_date")
            vehicle.ownership_date = data.get("ownership_date")
            vehicle.model_year = data.get("model_year")
            vehicle.sequence_number = data.get("sequence_number")

            vehicle.insert(ignore_permissions=True)
            transferred += 1

        except Exception as e:
            skipped.append(f"- Row: {license_plate or data.get('name')}, Error: {str(e)}")

    message = f"✅ Transferred {transferred} vehicles."
    if skipped:
        message += f"\n❌ Skipped {len(skipped)} due to issues:\n" + "\n".join(skipped)

    return message


def create_missing_vehicle_fields():
    doctype = "Vehicle"

    fields_to_add = [
        ("plate_type", "Data", "Plate Type"),
        ("branch_name", "Data", "Branch Name"),
        ("actual_driver_id", "Data", "Actual Driver ID"),
        ("actual_driver_name", "Data", "Actual Driver Name"),
        ("mvpi_status", "Data", "MVPI Status"),
        ("insurance_status", "Data", "Insurance Status"),
        ("restriction_status", "Data", "Restriction Status"),
        ("istemarah_issue_date", "Date", "Istemarah Issue Date"),
        ("vehicle_status", "Data", "Vehicle Status"),
        ("body_type", "Data", "Body Type"),
        ("inspection_expiry_date", "Date", "Inspection Expiry Date"),
        ("ownership_date", "Date", "Ownership Date"),
        ("model_year", "Int", "Model Year"),
        ("sequence_number", "Data", "Sequence Number"),

        # For layout improvements
        ("__break1", "Column Break", "Column Break"),
        ("__break2", "Section Break", "Additional Info"),
    ]

    for fieldname, fieldtype, label in fields_to_add:
        if not fieldname.startswith("__"):
            safe_create_custom_field(doctype, fieldname, fieldtype, label=label)
        else:
            safe_create_custom_field(doctype, fieldname, fieldtype, label=label, insert_after="license_plate")


def safe_create_custom_field(doctype, fieldname, fieldtype, **kwargs):
    full_fieldname = f"{doctype}-{fieldname}"
    existing = frappe.db.get_value("Custom Field", full_fieldname, ["name", "fieldtype"])
    
    if existing:
        existing_name, existing_type = existing
        if existing_type != fieldtype:
            frappe.log_error(
                title="Fieldtype Mismatch",
                message=f"⚠️ Cannot change fieldtype of '{fieldname}' from '{existing_type}' to '{fieldtype}'. "
                        f"Please delete manually via Customize Form."
            )
            return
        return

    custom_field = frappe.new_doc("Custom Field")
    custom_field.dt = doctype
    custom_field.fieldname = fieldname
    custom_field.fieldtype = fieldtype
    custom_field.label = kwargs.get("label", fieldname.replace("_", " ").title())
    custom_field.insert_after = kwargs.get("insert_after")
    custom_field.insert(ignore_permissions=True)



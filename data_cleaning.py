import requests
import pandas as pd


# API Endpoint
drivers_data = "https://data.montgomerycountymd.gov/resource/mmzv-x632.json"
non_motorist_data = "https://data.montgomerycountymd.gov/resource/n7fk-dce5.json"
incidents_data = "https://data.montgomerycountymd.gov/resource/bhju-22kf.json"
vehicle_reg_till_2022 = "https://opendata.maryland.gov/resource/kqkd-4fx8.json"
vehicle_reg_frm_2023_and_onwards = "https://opendata.maryland.gov/resource/db8v-9ewn.json"


# Function to fetch all data in one request (since SODA 2.1 has no limit)
def fetch_data(url):
    response = requests.get(f"{url}?$limit=9999999")  # Large limit to get all data
    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        print(f"❌ Error fetching data from {url}: {response.status_code}")
        return pd.DataFrame()


df_vehicle_frm_2023 = fetch_data(vehicle_reg_frm_2023_and_onwards)

# preping vehicle data frm 2023 and onwards
df_filtered_frm_2023 = df_vehicle_frm_2023[(df_vehicle_frm_2023["county"] == "MONTGOMERY") & (df_vehicle_frm_2023["year_month"].str.contains("/12", na=False))]
df_filtered_frm_2023 = df_filtered_frm_2023.copy()
df_filtered_frm_2023.loc[:, "year"] = df_filtered_frm_2023["year_month"].str.split("/").str[0].astype(int)
df_filtered_frm_2023.drop(columns=["year_month"], inplace=True)  
df_filtered_frm_2023.drop(columns=["county"], inplace=True)  
#print(df_filtered_frm_2023)

df_vehicle_till_2022 = fetch_data(vehicle_reg_till_2022)

# preping vehicle data till 2022
df_filtered_till_2022 = df_vehicle_till_2022[['fiscal_year', 'montgomery']]
df_filtered_till_2022 = df_filtered_till_2022.copy()
df_filtered_till_2022.loc[:, "year"] = df_filtered_till_2022["fiscal_year"].str.extract(r"(\d{4})").astype(int)
df_filtered_till_2022.drop(columns=["fiscal_year"], inplace=True)  # Remove FY column
df_filtered_till_2022.rename(columns={'montgomery': 'vehicle_count'}, inplace=True)
df_filtered_till_2022 = df_filtered_till_2022[df_filtered_till_2022["year"].astype(int) <= 2022]
#print(df_filtered_till_2022)

# merge both dataframes
df_merged = pd.concat([df_filtered_till_2022, df_filtered_frm_2023], ignore_index=True)
df_merged = df_merged.sort_values(by="year", ascending=True)
df_merged.to_csv("vehicle_registration.csv", index=False) 
#print("merged data:")
#print(df_merged)

df_drivers = fetch_data(drivers_data)
df_non_motorists = fetch_data(non_motorist_data)
df_incidents = fetch_data(incidents_data)

## clean non-motorist 
df_non_motorists.rename(columns={"at_fault":"non_motorist_at_fault"}, inplace=True)
# Clean pedestrian data by removing driver-related column
df_non_motorists.drop(columns=["driver_substance_abuse"], inplace=True)

## clean driver
# Clean driver data by removing pedestrian-related column
df_drivers.drop(columns=["non_motorist_substance_abuse"], inplace=True)


# Identify duplicate columns (ending with _x and _y)
def merge_duplicated_cols(dataset):
    for col in dataset.columns:
        if col.endswith("_x") and col.replace("_x", "_y") in dataset.columns:
            base_col = col[:-2]  # Remove "_x" suffix

            # Merge by filling NaN values from _x with values from _y
            dataset[base_col] = dataset[col].combine_first(dataset[col.replace("_x", "_y")])

            # Drop the original _x and _y columns
            dataset.drop(columns=[col, col.replace("_x", "_y")], inplace=True)
    # List of columns to exclude from title formatting
    # Apply title only to non-excluded columns
    exclude_cols = ["report_number", "local_case_number", "drivers_license_state","vehicle_id","vehicle_make","person_id"]  

    dataset = dataset.apply(lambda col: col.map(lambda x: x.title() if isinstance(x, str) and col.name not in exclude_cols else x))
    dataset.replace({"Other": "Unknown"}, inplace=True)
    dataset.replace({"N/A": "Not Applicable","None": "Not Applicable"}, inplace=True)

    return dataset

df_incidents = merge_duplicated_cols(df_incidents)

#clean road grade 
df_incidents["road_grade"] = df_incidents["road_grade"].str.strip().str.lower()
replace_road_grade= {
    r'\bgrade\b': '',
    r'\bhill crest\b': 'Hillcrest',
    r'\bhill uphill\b': 'Uphill',
    r'\bnot applicable\b': 'Level'
}
df_incidents["road_grade"] = df_incidents["road_grade"].replace(replace_road_grade,regex=True)



# first & second harmful event
def categorize_events(value):
    if pd.isna(value):  # Handle NaN values
        return "Unknown"
    value = str(value).strip().lower()

    ##not sure if i shld prioritise the first one or what sia.. (can create a column for each possible one but need to consider use case b4 doing all that work sia HAHA)
    if "animal" in value:
        return "Animal"
    elif "other non collision" in value or "other non-collision" in value:
        return "Unknown"
    elif "overturn" in value:
        return "Overturn"
    elif "bridge" in value:
        return "Bridge"
    elif "pedestrian" in value or "pedalcycle" in value or "bicycle" in value or "vision obstruction" in value or "non-motorist" in value:
        return "Non-Motorist"
    elif "guardrail" in value :
        return "Guardrail"
    elif "railway" in value or "road under construction/maintenance" in value:
        return "Railway Vehicle"
    elif "cargo" in value :
        return "Cargo Related"
    elif "traffic" in value or "cable barrier" in value or "crash cushion" in value or "curb" in value :
        return "Traffic Infrastructure"
    elif "fixed object" in value or "pole" in value or "fence" in value or "mailbox" in value or "other object" in value:
        return "Fixed Objects"
    elif "ditch" in value or "embankment" in value or "immersion" in value or "tree" in value:
        return "Environmental Hazards"
    elif "fell jumped from motor vehicle" in value:
        return "Fell from Motor Vehicle"
    
    elif "vehicle" in value or "conveyance":
        return "Other Road Vehicle"
    
    else:
        return value.title()
df_incidents["first_harmful_event"] = df_incidents["first_harmful_event"].apply(categorize_events)
df_incidents["second_harmful_event"] = df_incidents["second_harmful_event"].apply(categorize_events)

# junction 
df_incidents["junction"] = df_incidents["junction"].apply(lambda x: "Crossover Related" if "crossover" in str(x).lower() else x)
df_incidents["junction"] = df_incidents["junction"].apply(lambda x: "Intersection Related" if "intersection" in str(x).lower() else x)
df_incidents["junction"] = df_incidents["junction"].apply(lambda x: "Driveway Related" if "driveway" in str(x).lower() else x)
df_incidents["junction"] = df_incidents["junction"].apply(lambda x: "Unknown" if "other location not listed" in str(x).lower() else x)


# intersection type
df_incidents["intersection_type"] = df_incidents["intersection_type"].apply(lambda x: "Roundabout" if any(term in str(x).lower() for term in ["roundabout", "traffic circle"]) else x)

#road condition
df_incidents["road_condition"] = df_incidents["road_condition"].apply(lambda x: "Uneven Surface" if any(term in str(x).lower() for term in ["holes", "ruts"]) else x)
df_incidents["road_condition"]= df_incidents["road_condition"].apply(lambda x: "No Defects" if "not applicable" in str(x).lower() else x)

#only taking the useful cols
cols_from_incidents = df_incidents[["hit_run","road_grade","first_harmful_event","second_harmful_event","junction","intersection_type","road_condition","report_number","local_case_number"]]

#END merge drivers and non_motorists first
df_drivers["report_number"].astype(str).str.strip().str.lower()
df_non_motorists["report_number"].astype(str).str.strip().str.lower()
df_drivers["local_case_number"].astype(str).str.strip().str.lower()
df_drivers["local_case_number"].astype(str).str.strip().str.lower()

df_merged = df_drivers.merge(df_non_motorists, on=["report_number", "local_case_number"], how="left")
df_merged = merge_duplicated_cols(df_merged)

## vehicle movement
df_merged["vehicle_movement"] = df_merged["vehicle_movement"].apply(lambda x: "Making U-Turn" if "making u turn" in str(x).lower() else x)
df_merged["vehicle_movement"] = df_merged["vehicle_movement"].apply(lambda x: "Turning" if any(term in str(x).lower() for term in ["making left turn", "making right turn", "turning right", "turning left","right turn on red"]) else x)
df_merged["vehicle_movement"] = df_merged["vehicle_movement"].apply(lambda x: "Changing Lanes" if any(term in str(x).lower() for term in ["entering traffic lane", "leaving traffic lane"]) else x)
df_merged["vehicle_movement"] = df_merged["vehicle_movement"].apply(lambda x: "Overtaking" if any(term in str(x).lower() for term in ["overtaking/passing", "passing"]) else x)
df_merged["vehicle_movement"] = df_merged["vehicle_movement"].apply(lambda x: "Stopping" if any(term in str(x).lower() for term in ["stopping", "stopped"]) else x)
df_merged["vehicle_movement"] = df_merged["vehicle_movement"].apply(lambda x: "Starting Movement" if any(term in str(x).lower() for term in ["starting", "stopped"]) else x)
df_merged["vehicle_movement"] = df_merged["vehicle_movement"].apply(lambda x: "Starting Movement" if any(term in str(x).lower() for term in ["starting", "stopped"]) else x)
df_merged["vehicle_movement"] = df_merged["vehicle_movement"].apply(lambda x: "Not Applicable" if "parked" in str(x).lower() else x)
df_merged["vehicle_movement"] = df_merged["vehicle_movement"].apply(lambda x: "Driverless Moving Vehicle" if "driverless moving veh." in str(x).lower() else x)


# vehicle_damage_extent
df_merged["vehicle_damage_extent"] = df_merged["vehicle_damage_extent"].apply(lambda x: "No Damage" if "not applicable" in str(x).lower() else x)


## circumstance cleaning 
def categorize_circumstance(value):
    if pd.isna(value):  # Handle NaN values
        return "Unknown"
    value = str(value).strip().lower()
    parts = value.split(",")

    if parts[0] == "n/a" and len(parts) > 1:
        value = parts[1].strip()
    else:
        value = parts[0].strip()

    ##not sure if i shld prioritise the first one or what sia.. (can create a column for each possible one but need to consider use case b4 doing all that work sia HAHA)
    if "animal" in value:
        return "Animal"
    elif "backup due to non-recurring incident" in value:
        return "Backup Due To Non-Recurring Incident"
    elif "backup due to prior crash" in value:
        return "Backup Due To Prior Crash"
    elif "backup due to regular congestion" in value:
        return "Backup Due To Regular Congestion"
    elif "blowing sand" in value or "debris or obstruction" in value or "smog" in value or "vision obstruction" in value:
        return "Debris/Obstruction"
    elif "icy or snow-covered" in value or "sleet" in value:
        return "Wintry Conditions"
    elif "non-highway work" in value or "road under construction/maintenance" in value:
        return "Road Maintenance"
    elif "physical obstruction(s)" in value :
        return "Physical Obstruction"
    elif "rain" in value :
        return "Rainy Conditions"
    elif "ruts" in value or "shoulders low" in value or "worn" in value:
        return "Poor Road Surface Conditions"
    elif "severe crosswinds" in value :
        return "Extreme Weather Conditions"
    elif "toll booth" in value or "traffic control device" in value:
        return "Poor Traffic Control Infrastructure"
    
    elif "v exhaust system" in value or "v wipers" in value:
        return "Vehicle-Related Mechanical Problem"
    elif "wet" in value :
        return "Wet Conditions"
    
    else:
        return value.title()

df_merged["circumstance"] = df_merged["circumstance"].apply(categorize_circumstance)


df_merged["circumstance"] = df_merged["circumstance"].apply(lambda x: "Reckless Driving" if any(term in str(x).lower() for term in ["reckless", "inattentive", "careless", "aggressive", "negligent"]) else x)
df_merged["circumstance"] = df_merged["circumstance"].apply(lambda x: "Improper Driving Maneuvers" if "improper" in str(x).lower() else x)
df_merged["circumstance"] = df_merged["circumstance"].apply(lambda x: "Tailgating" if "followed too closely" in str(x).lower() else x)
df_merged["circumstance"] = df_merged["circumstance"].apply(lambda x: "Swerving" if any(term in str(x).lower() for term in ["ran off","swerved","too fast for conditions"]) else x)
df_merged["circumstance"] = df_merged["circumstance"].apply(lambda x: "Lane Discipline" if any(term in str(x).lower() for term in ["failed to keep in proper lane", "over-correcting","over-steering","wrong side"]) else x)
df_merged["circumstance"] = df_merged["circumstance"].apply(lambda x: "Right-of-Way Violations" if any(term in str(x).lower() for term in ["yield right-of-way"]) else x)
df_merged["circumstance"] = df_merged["circumstance"].apply(lambda x: "Traffic Signal Violations" if any(term in str(x).lower() for term in ["red light","stop sign","traffic sign"]) else x)

#pedestrian_action
df_merged["pedestrian_actions"] = df_merged["pedestrian_actions"].apply(lambda x: "Failure to Obey to Traffic Controls" if "failure to obey traffic signs" in str(x).lower() else x)


 # clean weather
df_merged.loc[:, "weather"] = df_merged.loc[:, "weather"].apply(lambda x: "Wintry Mix" if any(term in str(x).lower() for term in ["snow", "freezing rain", "freezing drizzle", "sleet", "hail"]) else x)
df_merged.loc[:, "weather"] = df_merged.loc[:, "weather"].apply(lambda x: "Foggy" if any(term in str(x).lower() for term in ["fog", "foggy"]) else x)
df_merged.loc[:, "weather"] = df_merged.loc[:, "weather"].apply(lambda x: "Severe Winds" if any(term in str(x).lower() for term in ["winds", "crosswinds"]) else x)
df_merged.loc[:, "weather"] = df_merged.loc[:, "weather"].apply(lambda x: "Rain" if any(term in str(x).lower() for term in ["raining"]) else x)
df_merged.loc[:, "weather"] = df_merged.loc[:, "weather"].apply(lambda x: "Unknown" if any(term in str(x).lower() for term in ["not applicable"]) else x)

#clean light
df_merged["light"] = df_merged["light"].apply(lambda x: "Dark - Lighted" if "dark lights on" in str(x).lower() else x)
df_merged["light"] = df_merged["light"].apply(lambda x: "Dark - Not Lighted" if "dark no lights" in str(x).lower() else x)
df_merged["light"] = df_merged["light"].apply(lambda x: "Dark - Unknown Lighting" if "dark -- unknown lighting" in str(x).lower() else x)
df_merged["light"] = df_merged["light"].apply(lambda x: "Unknown" if "not applicable" in str(x).lower() else x)


#clean surface condition
df_merged["surface_condition"] = df_merged["surface_condition"].apply(lambda x: "Ice" if "ice/frost" in str(x).lower() else x)
df_merged["surface_condition"] = df_merged["surface_condition"].apply(lambda x: "Water (Standing/Moving)" if "water (standing, moving)" in str(x).lower() else x)
df_merged["surface_condition"] = df_merged["surface_condition"].apply(lambda x: "Water (Standing/Moving)" if "water(standing/moving)" in str(x).lower() else x)

#related non-motorists
df_merged["related_non_motorist"] = df_merged["related_non_motorist"].str.replace("(Electric)", "", regex=False)
df_merged["related_non_motorist"] = df_merged["related_non_motorist"].str.replace("(Non-Electric)", "", regex=False)
df_merged["related_non_motorist"] = df_merged["related_non_motorist"].str.replace("In Animal-Drawn Veh", "In Animal-Drawn Vehicle", regex=False)

df_merged["related_non_motorist"] = df_merged["related_non_motorist"].str.replace("Other Pedestrian (Person In A Building, Skater, Personal Conveyance, Etc.)", "Pedestrian", regex=False)
df_merged["related_non_motorist"] = df_merged["related_non_motorist"].str.replace("Other Conveyance", "Unknown", regex=False)
df_merged["related_non_motorist"] = df_merged["related_non_motorist"].str.replace("Other Pedalcyclist", "Cyclist", regex=False)
df_merged["related_non_motorist"] = df_merged["related_non_motorist"].str.replace("Bicyclist", "Cyclist", regex=False)
df_merged["related_non_motorist"] = df_merged["related_non_motorist"].str.replace("Unknown Type Of Non-Motorist", "Unknown", regex=False)
df_merged["related_non_motorist"] = df_merged["related_non_motorist"].str.strip().str.title()

#traffic control
replace_traffic_control= {
    r'\bNo Controls\b': 'Not Applicable',
    r'\bFlashing Traffic Control Signal\b': 'Flashing Traffic Signal',
    r'\bPerson (including flagger, law enforcement, crossing guard, etc.)\b': 'Person',
    r'\bOther Signal\b': 'Traffic Signal',
    r'\bOther Warning Sign\b': 'Warning Sign',
}
df_merged["traffic_control"] = df_merged["traffic_control"].replace(replace_traffic_control,regex=True)
df_merged["traffic_control"] = df_merged["traffic_control"].apply(lambda x: "Traffic Signals" if any(term in str(x).lower() for term in ["traffic signal", "traffic control signal","lane use control signal"]) else x)
df_merged["traffic_control"] = df_merged["traffic_control"].apply(lambda x: "Warning Signs" if any(term in str(x).lower() for term in ["warning sign"]) else x)
df_merged["traffic_control"] = df_merged["traffic_control"].apply(lambda x: "Regulatory Signs" if any(term in str(x).lower() for term in ["stop sign","yield sign"]) else x)
df_merged["traffic_control"] = df_merged["traffic_control"].apply(lambda x: "School Zone" if any(term in str(x).lower() for term in ["school zone"]) else x)
df_merged["traffic_control"] = df_merged["traffic_control"].apply(lambda x: "Pedestrian Related" if any(term in str(x).lower() for term in ["pedestrian","person"]) else x)
df_merged["traffic_control"] = df_merged["traffic_control"].apply(lambda x: "Railroad-Related" if any(term in str(x).lower() for term in ["railway","railroad"]) else x)

#clean route type
df_merged["route_type"] = df_merged["route_type"].str.replace("Route", "", regex=False).str.strip()
df_merged["route_type"] = df_merged["route_type"].apply(lambda x: "State/Federal" if "(state)" in str(x).lower() else x)
df_merged["route_type"] = df_merged["route_type"].apply(lambda x: "Access Roads" if any(term in str(x).lower() for term in ["ramp", "spur","service road","crossover"]) else x)
df_merged["route_type"] = df_merged["route_type"].apply(lambda x: "Public Roadway" if any(term in str(x).lower() for term in ["public roadway","government"]) else x)
df_merged["route_type"] = df_merged["route_type"].apply(lambda x: "Local / County" if any(term in str(x).lower() for term in ["local","county","municipality"]) else x)


#clean agency_name
df_merged["agency_name"] = df_merged["agency_name"].apply(lambda x: "Gaithersburg Police Department" if "gaithersburg" in str(x).lower() else x)
df_merged["agency_name"] = df_merged["agency_name"].apply(lambda x: "Montgomery County Police Department" if "montgomery" in str(x).lower() else x)
df_merged["agency_name"] = df_merged["agency_name"].apply(lambda x: "Takoma Park Police Department" if "takoma" in str(x).lower() else x)
df_merged["agency_name"] = df_merged["agency_name"].apply(lambda x: "Rockville Police Department" if "rockville" in str(x).lower() else x)
df_merged["agency_name"] = df_merged["agency_name"].apply(lambda x: "Maryland National Capital Park Police Department" if any(term in str(x).lower() for term in ["mcpark", "maryland-national capital"]) else x)

#clean safety_equipment
df_merged["safety_equipment"] = df_merged["safety_equipment"].apply(lambda x: "Not Applicable" if "none" in str(x).lower() else x)

df_merged["safety_equipment"] = df_merged["safety_equipment"].apply(lambda x: "Helmet" if "mc/bike helmet" in str(x).lower() else x)
df_merged["safety_equipment"] = df_merged["safety_equipment"].str.replace("Protective Pads (Elbows, Knees, Shins, Etc.)", "Protective Pads", regex=False).str.strip()
df_merged["safety_equipment"] = df_merged["safety_equipment"].str.replace("Reflective Wear (Backpack, Triangles, Etc.)", "Reflective Gear", regex=False).str.strip()
df_merged["safety_equipment"] = df_merged["safety_equipment"].str.replace("Reflectors", "Reflective Gear", regex=False).str.strip()
df_merged["safety_equipment"] = df_merged["safety_equipment"].str.replace("Reflective Clothing", "Reflective Gear", regex=False).str.strip()

df_merged["safety_equipment"] = df_merged["safety_equipment"].apply(
    lambda x: ", ".join(sorted(set(map(str.strip, str(x).split(", "))))) if pd.notna(x) else x
)


#pedestrian_location
df_merged["pedestrian_location"] = df_merged["pedestrian_location"].apply(lambda x: "Shoulder" if "shoulder" in str(x).lower() else x)
df_merged["pedestrian_location"] = df_merged["pedestrian_location"].apply(lambda x: "Median" if "median" in str(x).lower() else x)
df_merged["pedestrian_location"] = df_merged["pedestrian_location"].apply(lambda x: "Bike Lanes" if "bike lanes" in str(x).lower() else x)
df_merged["pedestrian_location"] = df_merged["pedestrian_location"].apply(lambda x: "Shared Lane Markings" if "shared lane markings)" in str(x).lower() else x)

#clean pedestrian_actions
def categorize_action(value):
    if pd.isna(value):  # Handle NaN values
        return "Unknown"
    value = str(value).split(",")[0].strip().lower()

    ##not sure if i shld prioritise the first one or what sia.. (can create a column for each possible one but need to consider use case b4 doing all that work sia HAHA)
    if "dart" in value and "dash" in value:
        return "Dart/Dash"
    elif "failure to yield" in value or "improper passing" in value or "improper turn" in value:
        return "Failure to Yield"
    elif "in roadway improperly" in value:
        return "In Roadway Improperly"
    elif "disabled vehicle" in value:
        return "Disabled Vehicle Related"
    elif "entering/exiting" in value:
        return "Entering/Exiting Vehicle"
    elif "inattentive" in value:
        return "Inattentive Behavior"
    elif "not visible" in value:
        return "Not Visible"
    elif "no improper action" in value or "none"  in value:
        return "Not Applicable"
    elif "wrong-way" in value or "wrong way" in value:
        return "Wrong-Way Movement"
    

df_merged["pedestrian_actions"] = df_merged["pedestrian_actions"].apply(categorize_action)

#clean pedestrian_movement
df_merged["pedestrian_movement"] = df_merged["pedestrian_movement"].apply(lambda x: "Unknown" if "other working" in str(x).lower() else x)
df_merged["pedestrian_movement"] = df_merged["pedestrian_movement"].apply(lambda x: "Walking/Riding Against Traffic" if "walking/cycling along roadway against traffic" in str(x).lower() else x)
df_merged["pedestrian_movement"] = df_merged["pedestrian_movement"].apply(lambda x: "Walking/Riding With Traffic" if "walking/cycling along roadway with traffic" in str(x).lower() else x)
df_merged["pedestrian_movement"] = df_merged["pedestrian_movement"].apply(lambda x: "Walking/Riding With Traffic" if "walking/riding w/traffic" in str(x).lower() else x)


# clean pedestrian_type
df_merged["pedestrian_type"] = df_merged["pedestrian_type"].apply(lambda x: "Cyclist" if "cyclist" in str(x).lower() else x)
df_merged["pedestrian_type"] = df_merged["pedestrian_type"].apply(lambda x: "Wheelchair" if "wheelchair" in str(x).lower() else x)
df_merged["pedestrian_type"] = df_merged["pedestrian_type"].apply(lambda x: "Scooter" if "scooter" in str(x).lower() else x)
df_merged["pedestrian_type"] = df_merged["pedestrian_type"].apply(lambda x: "Unknown" if "other conveyance" in str(x).lower() else x)
df_merged["pedestrian_type"] = df_merged["pedestrian_type"].apply(lambda x: "Pedestrian" if "pedestrian" in str(x).lower() else x)
df_merged["pedestrian_type"] = df_merged["pedestrian_type"].apply(lambda x: "Unknown" if "unknown" in str(x).lower() else x)
df_merged["pedestrian_type"] = df_merged["pedestrian_type"].replace("In Animal-Drawn Veh", "In Animal-Drawn Vehicle", regex=False)

# clean driver_distracted_by
df_merged["driver_distracted_by"] = df_merged["driver_distracted_by"].apply(lambda x: "Cellular Phone" if "cellular phone" in str(x).lower() else x)
df_merged["driver_distracted_by"] = df_merged["driver_distracted_by"].apply(lambda x: "Distraction Outside Vehicle"  if "outside" in str(x).lower() else x)
df_merged["driver_distracted_by"] = df_merged["driver_distracted_by"].apply(lambda x: "Distraction Inside Vehicle"  if any(term in str(x).lower() for term in ["in vehicle", "occupants"]) else x)
df_merged["driver_distracted_by"] = df_merged["driver_distracted_by"].apply(lambda x: "Unknown" if any(term in str(x).lower() for term in ["other distraction", "other action"]) else x)
df_merged["driver_distracted_by"] = df_merged["driver_distracted_by"].apply(lambda x: "Vehicle Control Distraction" if any(term in str(x).lower() for term in ["adjusting audio", "controls integral to vehicle"]) else x)
df_merged["driver_distracted_by"] = df_merged["driver_distracted_by"].apply(lambda x: "Other Electronic Devices"  if any(term in str(x).lower() for term in ["device", "manually operating"]) else x)

# clean vehicle_first_impact_location
df_merged["vehicle_first_impact_location"] = df_merged["vehicle_first_impact_location"].str.replace(r"(?i)\boclock\b", "O Clock", regex=True)
df_merged["vehicle_first_impact_location"] = df_merged["vehicle_first_impact_location"].apply(lambda x: "Front"  if any(term in str(x).lower() for term in ["twelve"]) else x)
df_merged["vehicle_first_impact_location"] = df_merged["vehicle_first_impact_location"].apply(lambda x: "Rear"  if any(term in str(x).lower() for term in ["six"]) else x)
df_merged["vehicle_first_impact_location"] = df_merged["vehicle_first_impact_location"].apply(lambda x: "Right"  if any(term in str(x).lower() for term in ["one","two","three","four","five"]) else x)
df_merged["vehicle_first_impact_location"] = df_merged["vehicle_first_impact_location"].apply(lambda x: "Left"  if any(term in str(x).lower() for term in ["seven","eight","nine","ten","eleven"]) else x)
df_merged["vehicle_first_impact_location"] = df_merged["vehicle_first_impact_location"].apply(lambda x: "Top"  if any(term in str(x).lower() for term in ["roof top"]) else x)


# clean vehicle_body_type
def categorize_vehicle(value):
    if pd.isna(value):
        #idk abt this yea, why wld there be n/a, dk if i shld put unknown anot
        return "Not Applicable" 
    value = str(value).lower()

    if any(term in value for term in ["passenger car", "utility vehicle", "station wagon", "limousine"]):
        return "Passenger Vehicles"
    elif any(term in value for term in ["van", "cargo van","pickup truck","light trucks","pickup","light truck"]):
        return "Vans & Light Truck"
    elif any(term in value for term in ["bus", "transit"]):
        return "Buses & Public Transport"
    elif any(term in value for term in ["motorcycle", "moped", "autocycle"]):
        return "Motorcycles & Mopeds"
    elif any(term in value for term in ["ambulance", "fire vehicle", "police vehicle"]):
        return "Public Safety Vehicles"
    elif any(term in value for term in ["heavy trucks", "truck tractor", "single-unit","other trucks"]):
        return "Heavy & Commercial Trucks"
    elif any(term in value for term in ["construction", "farm"]):
        return "Construction & Farm Vehicles"
    elif any(term in value for term in ["atv", "golf cart", "snowmobile", "recreational","low speed"]):
        return "Off-Road & Recreational Vehicles"
    else:
        return value.title()

df_merged["vehicle_body_type"] = df_merged["vehicle_body_type"].apply(categorize_vehicle)

#clean vehicle_going_dir
replace_vehicle_going_dir = {
    r'\bNorth\b': 'Northbound',
    r'\bSouth\b': 'Southbound',
    r'\bEast\b': 'Eastbound',
    r'\bWest\b': 'Westbound'
}
df_merged["vehicle_going_dir"] = df_merged["vehicle_going_dir"].replace(replace_vehicle_going_dir,regex=True)

# clean collision_type
df_merged["collision_type"] = df_merged["collision_type"].apply(lambda x: "Rear-End Collisions" if any(term in str(x).lower() for term in ["rear", "rend"]) else x)
df_merged["collision_type"] = df_merged["collision_type"].apply(lambda x: "Angle Collisions" if "angle" in str(x).lower() else x)
df_merged["collision_type"] = df_merged["collision_type"].apply(
    lambda x: "Same Direction Sideswipes" if "sideswipe" in str(x).lower() and "same direction" in str(x).lower() else x
)
df_merged["collision_type"] = df_merged["collision_type"].apply(
    lambda x: "Opposite Direction Sideswipes" if "sideswipe" in str(x).lower() and "opposite direction" in str(x).lower() else x
)

df_merged["collision_type"] = df_merged["collision_type"].apply(lambda x: "Head-On Collisions"  if any(term in str(x).lower() for term in ["head on", "front to front"]) else x)
df_merged["collision_type"] = df_merged["collision_type"].apply(lambda x: "Turning Collisions" if "turn" in str(x).lower() else x)



# clean driver & non-motorist substance abuse                            
def categorize_substance(value):
    if pd.isna(value) :  # Handle NaN and empty values
        return ["", ""]  # Assume no substance use if missing

    value = str(value).lower()  # Convert to lowercase string

    if ", " in value:  # If a comma exists, split normally
        parts = value.split(", ")

        # Debugging print for cases where more than 2 columns are found
        if len(parts) > 2:
            print(f"⚠️ Found more than 2 values: {parts}")  

        return parts[:2]
    elif "alcohol" in value:  # If it mentions alcohol but no comma
        return ["Yes", "No"]
    elif "drug" in value or "medication" in value:  # Check both correctly
        return ["No", "Yes"]
    elif "combination" in value or "combine" in value:
        return ["Yes", "Yes"]
    elif "not applicable" in value or "none" in value:
        return ["No", "No"]
    else:  # If neither alcohol nor drug is mentioned
        return [value, value]  


df_merged[['non_motorist_alcohol_use', 'non_motorist_drug_use']] = pd.DataFrame(
    df_merged['non_motorist_substance_abuse'].apply(categorize_substance).tolist(), index=df_merged.index
)

df_merged[['driver_alcohol_use', 'driver_drug_use']] = pd.DataFrame(
    df_merged['driver_substance_abuse'].apply(categorize_substance).tolist(), index=df_merged.index
)

df_merged[["driver_alcohol_use", "driver_drug_use",'non_motorist_alcohol_use', 'non_motorist_drug_use']] = df_merged[["driver_alcohol_use", "driver_drug_use",'non_motorist_alcohol_use', 'non_motorist_drug_use']].apply(
    lambda col: col.map(lambda x: "Not Suspected" if "not suspect of" in str(x).lower() else x)
)

df_merged[["driver_alcohol_use", "driver_drug_use",'non_motorist_alcohol_use', 'non_motorist_drug_use']] = df_merged[["driver_alcohol_use", "driver_drug_use",'non_motorist_alcohol_use', 'non_motorist_drug_use']].apply(
    lambda col: col.map(lambda x: "Suspected" if "suspect of" in str(x).lower() else x)
)

df_merged[["driver_alcohol_use", "driver_drug_use",'non_motorist_alcohol_use', 'non_motorist_drug_use']] = df_merged[["driver_alcohol_use", "driver_drug_use",'non_motorist_alcohol_use', 'non_motorist_drug_use']].apply(
   lambda col: col.map(lambda x: "Unknown" if "unknown" in str(x).lower() else x)
)

df_merged[["driver_alcohol_use", "driver_drug_use",'non_motorist_alcohol_use', 'non_motorist_drug_use']] = df_merged[["driver_alcohol_use", "driver_drug_use",'non_motorist_alcohol_use', 'non_motorist_drug_use']].apply(
   lambda col: col.map(lambda x: "Unknown" if "other" in str(x).lower() else x)
)


#clean off-road descrp
df_merged.loc[:, "off_road_description"] = df_merged.loc[:, "off_road_description"].apply(lambda x: "Parking Lot" if "parking lot" in str(x).lower() else x)


#final clean + merging
df_merged.drop(columns=["driver_substance_abuse", "non_motorist_substance_abuse"], errors="ignore", inplace=True)

final_merged = df_merged.merge(cols_from_incidents,on=["report_number", "local_case_number"], how="left")
final_merged = merge_duplicated_cols(final_merged)

final_merged = final_merged.drop(columns=["geolocation"], errors="ignore")  # Drop and ignore errors if column doesn't exist
final_merged = final_merged.loc[:, ~final_merged.columns.str.contains("computed_region")]
final_merged['year'] = pd.to_datetime(final_merged['crash_date_time']).dt.year  ## extract the year
final_merged = final_merged.drop_duplicates()
pd.set_option("display.max_columns", None)  # Show all columns

final_merged.to_csv("final_modified_file.csv", index=False)  # Set index=False to avoid writing row indices

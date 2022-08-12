from dataclasses import dataclass

@dataclass
class item_class:
    Product_ID: str
    ISBN13: str
    Company_No: str
    Minor_Disc_ID: str
    Minor_Discipline_Description: str
    BISAC1: str
    BISAC2: str
    BISAC3: str
    Type_ID: str
    Title: str
    Publication_Status_ID: str
    Series_ID: str
    Season: str
    Editor: str
    First_Due_Date: str
    Interest_Code: str
    Rights_Code: str
    Rights_Areas: str
    Can_Distribute_Countries_Country_ID: str
    Can_Distribute_Countries_Country_Name: str
    Can_Distribute_Countries_Assoc_Country_Group: str
    Cannot_Distribute_Countries_Assoc_Country_Group: str
    Cannot_Distribute_Countries_Country_ID: str
    Cannot_Distribute_Countries_Country_Name: str
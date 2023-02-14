import mysql.connector
from datetime import datetime, date
from dateutil import relativedelta
import nepali_datetime #https://github.com/dxillar/nepali-datetime


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="esewa_ss_rc"
)

mycursor = mydb.cursor()


# Returns date as array. example: [yyyy, MM, dd]
def getDateArray(dateString):
  if "-" in dateString:
    return dateString.split("-")
  elif "/" in dateString:
    return dateString.split("/")
  else:
    return None


def isStringLength1Or2(str):
    return len is not None and (len(str) == 1 or len(str) == 2) 


# Date array should be one of below:
    # [2022, 01, 01] , [2022, 1, 1], [1, 1, 2021]
# returns array [yyyy, MM, dd]
def validateDateArray(dateArray):
    if dateArray is None or len(dateArray) != 3:
        return None
    
    if len(dateArray[0]) == 4 and isStringLength1Or2(dateArray[1]) and isStringLength1Or2(dateArray[2]):
        return dateArray
    
    if isStringLength1Or2(dateArray[0]) and isStringLength1Or2(dateArray[1]) and len(dateArray[2]) == 4:
        year = dateArray[2]
        day = dateArray[0]
        return [year, dateArray[1], day]
    
    return None


def isDateAlreadyInAD(date, orgCreatedDate):
    diff = relativedelta.relativedelta(orgCreatedDate, date)
    if diff.years >= 16:
        return True
    return False

def getDateInAd(dobString, orgCreatedDate):
    dateArray = getDateArray(dobString) 
    print(dateArray)
    dateArray = validateDateArray(dateArray)
    
    if dateArray is None:
        return None

    year = int(dateArray[0])
    month = int(dateArray[1])
    day = int(dateArray[2])

    dob = date(year, month, day)
    print("year: ", year, "month: ", month, "day: ", day)

    if isDateAlreadyInAD(dob, orgCreatedDate):
        return dob

    print("converting to ad")
    return nepali_datetime.date(year, month, day).to_datetime_date()

def getAllOwners():
    selectOwners = "Select id, org_created_date, date_in_bs, date_in_ad from test;"
    mycursor.execute(selectOwners)
    return mycursor.fetchall()


def updateAllDateOfBirthToAD(owners):
    for owner in owners:
        ownerId = owner[0]
        orgCreatedDate = owner[1]
        dateInBs = owner[2]

        if dateInBs is not None and orgCreatedDate is not None:
            try:
                newDate = getDateInAd(dateInBs, orgCreatedDate)
                
                if newDate is None:
                    print("skipping row with id: ", ownerId, ". Could not convert ", dateInBs ,  "to AD")
                    continue

                print(newDate)

                updateQuery = "update test t set t.date_in_ad = %(newDate)s where t.id = %(id)s"
                mycursor.execute(updateQuery, {
                    'newDate': newDate,
                    'id': ownerId
                })
                print("Row updated of id: ", ownerId, ". Updated row count: ", mycursor.rowcount)    
            except Exception as ex:
                print(ex)


def main():
    owners = getAllOwners()
    print("First row ", owners[0][0])
    updateAllDateOfBirthToAD(owners)

main()



mydb.commit()


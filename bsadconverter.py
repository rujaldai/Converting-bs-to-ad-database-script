import mysql.connector
from datetime import datetime, date
from dateutil import relativedelta
from pyBSDate import convert_BS_to_AD #https://github.com/SushilShrestha/pyBSDate


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


def isDateAlreadyInAD(date):
    diff = relativedelta.relativedelta(datetime.now(), date)
    if diff.years >= 16:
        return True
    return False

def getDateInAd(dateString):
    dateArray = getDateArray(dateString) 
    print(dateArray)
    dateArray = validateDateArray(dateArray)
    
    if dateArray is None:
        return None

    year = dateArray[0]
    month = dateArray[1]
    day = dateArray[2]
    try:
        convertedDate = date(int(year), int(month), int(day))
    except:
        return None

    print("year: ", year, "month: ", month, "day: ", day)

    if isDateAlreadyInAD(convertedDate):
        return convertedDate

    dateInAd = convert_BS_to_AD(year, month, day)
    return date(int(dateInAd[0]), int(dateInAd[1]), int(dateInAd[2]))


    # print(date)
    # return datetime.date(year, month, day)


def getAllOwners():
    selectOwners = "Select * from test;"
    mycursor.execute(selectOwners)
    return mycursor.fetchall()


def updateAllDateOfBirthToAD(owners):
    for owner in owners:
        if owner[1] is not None:
            newDate = getDateInAd(owner[1])
            
            if newDate is None:
                print("skipping row with id: ", owner[0], ". Could not convert ", owner[1] ,  "to AD")
                continue

            print(newDate)

            updateQuery = "update test t set t.date_in_ad = %(newDate)s where t.id = %(id)s"
            mycursor.execute(updateQuery, {
                'newDate': newDate,
                'id': owner[0]
            })
            print("Row updated of id: ", owner[0], ". Updated row count: ", mycursor.rowcount)    


def main():
    owners = getAllOwners()
    print("First row ", owners[0][0])
    updateAllDateOfBirthToAD(owners)

main()



mydb.commit()


# print(mycursor.rowcount, "record(s) affected")

# sql = "insert into test(`date_in_bs`) values('2022-01-01');"
# selectOwners = "Select * from test;"
# mycursor.execute(selectOwners)
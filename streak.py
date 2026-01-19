#!.venv/bin/python

import os
import sys
import time
import json
from pathlib import Path
import shutil
from datetime import datetime, date, timedelta
from glob import glob

import requests
from loguru import logger
from jinja2 import Environment, FileSystemLoader


MAX_COUNT = 1000000


@logger.catch
def Convert(Line):
 Result = Line.strip().split(";")
 Id, ClosedAt, ChangesCount, UId, User = Result[:5]
 CreatedBy = ";".join(Result[5:-1])
 Locale = Result[-1]
 Id, ChangesCount, UId = int(Id) if Id else 0, int(ChangesCount) if ChangesCount else 0, int(UId) if UId else 0
 ClosedAt = datetime.fromisoformat(ClosedAt) if ClosedAt else None
 return Id, ClosedAt, ChangesCount, UId, User, CreatedBy, Locale


def Jinja(Result):
  Loader = FileSystemLoader(searchpath="./")
  Env = Environment(loader=Loader)
  Template = Env.get_template("streak.htm")
  Render = Template.render(Result)
  FileName = Path(f"index.html")
  with open(FileName, mode="w", encoding="utf-8") as File:
    File.write(Render)


# -= changeset =-


@logger.catch
def GetRequest(Date):
  URL = "https://www.openstreetmap.org/api/0.6/changesets.json"
  Params = {'from': "2000-01-01", 'to': Date}
  while True:
    try:
      Response = requests.get(URL, params=Params)
      if Response.status_code == 200:
        return Response.json()
      else:
        logger.error("Status code = {status_code}, Param = {param}", status_code=Response.status_code, param=Param)
        return {}
    except:
      logger.exception("Status code = {status_code}", status_code=Response.status_code)
      logger.info("pause 300 s")
      time.sleep(300)


def LoadProcess():
  logger.info("load process")
  FileName = Path(".data/process.json")
  if FileName.exists():
    with open(FileName, "r", encoding='utf-8') as File:
      Lines = File.readlines()
      Lines = "".join(Lines)
      return json.loads(Lines)
  else:
    Result = {
      'Start': None,
      'Finish': 1,
      'Current': None,
      'Date': (date.today() - timedelta(days=1)).isoformat(),
    }
    SaveProcess(Result)
    return Result


def SaveProcess(Json):
  FileName = Path(".data/process.json")
  with open(FileName, "w", encoding='utf-8') as File:
    json.dump(Json, File, indent=2, ensure_ascii=False, sort_keys=False)


def ParseChangeset(Process, Data):
  FileName = Path(".data/data.csv")
  #
  with open(FileName, "a", encoding='utf-8') as File:
    Tags = ['id', 'closed_at', 'changes_count', 'uid', 'user' ]
    for Item in Data:
      if Process['Start'] is None:
        Process['Start'] = Item['id']
      #
      if Item['id'] != Process['Current']:
        Result = [ str(Item.get(Tag, "")) for Tag in Tags ]
        ItemTags = Item.get('tags', {})
        Result.append(ItemTags.get('created_by', ""))
        Result.append(ItemTags.get('locale', ""))
        File.write(";".join(Result) + "\n")
      #
      if Item['id'] == Process['Finish']:
        Process['Finish'] = (Process['Start'] // MAX_COUNT - 1) * MAX_COUNT
        Process['Start'], Process['Current'], Process['Date'] = None, None, None
        SaveProcess(Process)
        return True
  #
  Process['Current'] = Data[-1]['id']
  Process['Date'] = Data[-1]['created_at']
  #
  SaveProcess(Process)
  return False


@logger.catch
def Changeset():
  logger.info("Changesets")
  Process = LoadProcess()
  #
  Index = 0
  while True:
    logger.info("process {id} {date}", id=Process['Current'], date=Process['Date'])
    Data = GetRequest(Process['Date'])
    if Data is None:
      logger.info("pause 60 s")
      time.sleep(60)
      continue
    if ParseChangeset(Process, Data['changesets']):
      logger.info("process done")
      break
    Index += 1
    if Index % 100000 == 99999:
      logger.info("pause 200 s")
      time.sleep(200)
    elif Index % 10000 == 9999:
      logger.info("pause 120 s")
      time.sleep(120)
    elif Index % 1000 == 999:
      logger.info("pause 60 s")
      time.sleep(60)
    elif Index % 100 == 99:
      logger.info("pause 30 s")
      time.sleep(30)
    elif Index % 10 == 9:
      logger.info("pause 10 s")
      time.sleep(10)


# -= split =-


def SaveSplit(Key, Lines):
  logger.info("save {key:04}", key=Key)
  FileName = Path(f".data/{Key:04}.data.csv")
  with open(FileName, "w", encoding='utf-8') as File:
    for Id in sorted(Lines.keys()):
      File.write(Lines[Id])


def Split():
  logger.info("Split")
  Temp = {}
  FileName = Path(".data/data.csv")
  with open(FileName, "r", encoding='utf-8') as File:
    for Line in File:
      #Id, ClosedAt, ChangesCount, UId, User, CreatedBy, Locale = Convert(Line)
      #print(f"{Id=}, {ClosedAt=}, {ChangesCount=}, {UId=}, {User=}, {CreatedBy=}, {Locale=}")
      Id, _, _, _, _, _, _ = Convert(Line)
      #
      Index = Id // MAX_COUNT
      if Index not in Temp:
        Temp[Index] = {}
      if Id in Temp[Index]:
        logger.warning("double '{line}'", line=Line.strip())
      Temp[Index][Id] = Line
      #
      if len(Temp) > 2:
        Key = next(iter(Temp))
        Lines = Temp.pop(Key)
        SaveSplit(Key, Lines)
  #
  for Key, Lines in Temp.items():
    SaveSplit(Key, Lines)
  #
  os.remove(FileName)


# -= Date =-


def SaveDate(ClosedAt, Array):
  if ClosedAt is not None:
    Date = ClosedAt.strftime("%Y-%m-%d")
    FileName = Path(f".data/{Date}.csv")
    with open(FileName, "a", encoding='utf-8') as File:
      for Item in Array:
        File.write(Item)


def Date():
  logger.info("Date")
  PathName = Path(".data").resolve()
  for FileName in PathName.rglob("????-??-??.csv"):
    os.remove(FileName)
  #
  Date, Array = None, []
  for FileName in sorted(PathName.rglob("????.data.csv")):
    logger.info("parse {filename}", filename=FileName.name)
    with open(FileName, "r", encoding='utf-8') as File:
      for Line in File:
        _, ClosedAt, _, _, _, _, _ = Convert(Line)
        if ClosedAt:
          ClosedAt = ClosedAt.date()
          if ClosedAt != Date:
            SaveDate(Date, Array)
            Date, Array = ClosedAt, []
          Array.append(Line)
  SaveDate(Date, Array)


# -= Streak =-


def ParseStreak():
  Array = {}
  PathName = Path(".data").resolve()
  for FileName in sorted(PathName.rglob("????-??-??.csv")):
    if FileName.name[-7:] == "-01.csv":
      logger.info("parse {filename}", filename=FileName.name)
    Date = FileName.stem
    if Date not in ["2024-12-15", "2024-12-17", ]:
      with open(FileName, "r", encoding='utf-8') as File:
        for Line in File:
          _, _, _, UId, User, _, _ = Convert(Line)
          if UId in Array:
            Item = Array[UId]
            Item['User'] = User
            if Date != Item['End']:
              Item['End'] = Date
              Item['Count'] += 1
              Item['Skip'] = False
            #Array[UId] = Item
          else:
            Array[UId] = {'UId': UId, 'User': User, 'Begin': Date, 'End': Date, 'Count': 1, 'Skip': False, }
      #
      for UId, Item in Array.copy().items():
        if Item['End'] != Date:
          if Item['Skip']:
            if Item['Count'] > 365:
              del Item['Skip']
              yield Item
            Array.pop(UId)
          else:
            Item['Skip'] = True
  #
  for UId, Item in Array.copy().items():
    if Item['Count'] > 365:
      del Item['Skip']
      yield Item


def Streak():
  logger.info("Streak")
  PathName = Path(".data").resolve()
  Date = sorted(PathName.rglob("????-??-??.csv"))[-1].stem
  CreatedAt = datetime.now().strftime("%Y-%m-%dT%H:%M:00Z")
  Streaks = list(ParseStreak())
  Streaks = sorted(Streaks, key=lambda Item: Item['Count'], reverse=True)
  for Index, Item in enumerate(Streaks):
    Item['Index'] = Index + 1
  #
  Template = {'Streaks': Streaks, 'CreatedAt': CreatedAt, 'Date': Date, }
  Jinja(Template)
  #
  PathName = Path(".data").resolve()
  for FileName in PathName.rglob("????-??-??.csv"):
    os.remove(FileName)


# -= Main =-


if __name__ == "__main__":
  sys.stdin.reconfigure(encoding="utf-8")
  sys.stdout.reconfigure(encoding="utf-8")
  #
  logger.add(Path(".log/streak.log"))
  logger.info("Start")
  Changeset()
  Split()
  Date()
  Streak()
  logger.info("Done")

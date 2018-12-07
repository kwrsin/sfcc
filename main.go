/*
Simple FX Candle Converter

Usage
  param1 : datafile
  param2 : filter 2001 => just excluded 2001year's data
  param3 : command => "sum" command converts new candle data from a input data file. other command(or empty) just exclues data with filter(2nd parameter).
  param4 : set seconds 10min(10*60) => 600, 1hour(60*60) => 3600

  ex:)
    ./sfcc  sample.txt 2001

    OR

    ./sfcc  sample.txt 2001 sum 600
*/

package main

import(
  "encoding/csv"
  "fmt"
  "io"
  "os"
  "log"
  "errors"
  "strings"
  "strconv"
)

const join_separator string = ","
const division_separator string = ","
const action_sum string = "sum"
const hour int = 60*60
const day int = hour*24
const month int = day*31
const year int = month*12

func main() {
  var command string = "search"
  var group_sec int = 60

  var fp *os.File
  var err error
  fmt.Println(os.Args)
  if len(os.Args) < 3  {
    failOnError(errors.New(fmt.Sprintf("", "too short args!")))
  }
  var pattern = os.Args[2]
  if len(pattern) < 4 {
    failOnError(errors.New(fmt.Sprintf("", "a date pattern is required!")))
  }
  if len(os.Args) >= 4 {
    command = os.Args[3]
  }
  if len(os.Args) >= 5 {
    group_sec, err = strconv.Atoi(os.Args[4])
    failOnError(err)
  }

  fp, err = os.Open(os.Args[1])
  if err != nil  {
    failOnError(err)
  }
  defer fp.Close()

  reader := csv.NewReader(fp)
  reader.Comma = []rune(division_separator)[0]
  reader.LazyQuotes = true
  mergedDataDict := make(map[string][]string)
  prevKey := ""
  for {
    record, err := reader.Read()
    if err == io.EOF  {
      break
    } else if err != nil {
      failOnError(err)
    }
    if command == action_sum  {
      if(pattern == record[1][0:len(pattern)] && isNumber(record[1][0:4])) {
        div, unit := getNumberOfDivision(group_sec)
        datetime_str := record[1] + record[2]
        key := getKey(datetime_str, div, unit)
        if prevKey != "" && mergedDataDict[key] == nil {
          fmt.Println(strings.Join(mergedDataDict[prevKey], join_separator))
          delete(mergedDataDict, prevKey)
          prevKey = ""
        }
        if mergedDataDict[key] == nil {
          mergedDataDict[key] = record
        } else {
          mergedDataDict[key] = merge_data(record, mergedDataDict[key])
        }

        prevKey = key
      }
    } else {
      if(pattern == record[1][0:len(pattern)]) {
        fmt.Println(strings.Join(record, join_separator))
      }
    }
  }
      breakpoint();
  if prevKey != "" && mergedDataDict[prevKey] != nil {
    fmt.Println(strings.Join(mergedDataDict[prevKey], join_separator))
    delete(mergedDataDict, prevKey)
    prevKey = ""
  }
}

func failOnError(err error) {
  if err != nil  {
    log.Fatal("Error: ", err)
  }
}

func merge_data(record []string, data []string) (ret []string) {
  rPair := record[0]
  rDate := record[1]
  rTime := record[2]
  rHigh := record[4]
  rLow := record[5]
  rClose := record[6]
  rVol:= record[7]

  sOpen := data[3]
  sHigh := data[4]
  sLow := data[5]
  sVol:= data[7]
  if sHigh > rHigh {
    rHigh = sHigh
  }
  if sLow < rLow {
    rLow = sLow
  }

  rVol = toString(toNumber(rVol) + toNumber(sVol))

  return []string{rPair, rDate, rTime, sOpen, rHigh, rLow, rClose, rVol}
}

func getKey(datetime_str string, div int, unit int ) (key string) {
  switch (unit) {
  case hour:
    var val, _ = strconv.Atoi(datetime_str[10:12])
    ret := datetime_str[0:10] + fmt.Sprintf("%02d", val / (60 / div))
    return ret
  case day:
    var val, _ = strconv.Atoi(datetime_str[8:10])
    ret := datetime_str[0:8] + fmt.Sprintf("%02d", val / (24 / div))
    return ret
  case month:
    var val, _ = strconv.Atoi(datetime_str[6:8])
    ret := datetime_str[0:6] + fmt.Sprintf("%02d", val / (31 / div))
    return ret
  default: //year:
    var val, _ = strconv.Atoi(datetime_str[4:6])
    ret := datetime_str[0:4] + fmt.Sprintf("%02d", val / (12 / div))
    return ret
  }
}

func getNumberOfDivision(step_sec int) (div int, unit int){
  if(step_sec < hour) {
    return hour / step_sec, hour
  } else if(step_sec < day) {
    return day / step_sec, day
  } else if(step_sec < month) {
    return month / step_sec, month
  }
  return year / step_sec, year
}

func isNumber(val string) bool {
  if _, err := strconv.Atoi(val); err == nil {
    return true
  }
  return false
}

func toNumber(val string) int {
  v, _ := strconv.Atoi(val)
  return v
}

func toString(val int) string {
  return strconv.Itoa(val)
}

func breakpoint() {
  return
}


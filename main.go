/*
Simple FX Candle Converter

Usage
  param1 : datafile(csv, tsv...)
  param2 : filter 2018 => just excluded 2018 year's data.
  param3 : command => "sum" command converts new candle data from a input data file(1 minite). other command(or empty) just exclues data with filter(2nd parameter).
  param4 : set seconds 10min(10*60) => 600, 1hour(60*60) => 3600

  ex:)
    ./sfcc  sample.txt 2018 > usjp2018.txt

    OR

    ./sfcc sample.txt 2018 sum $((60*60)) > usjp2018.txt

  Options
    you can use options.(opts.json: the file need to put same directory app file exist.)
    order => order of reading file
    division_separator => input's separator
    join_separator => output's separator
*/

package main

import(
  "encoding/csv"
  "encoding/json"
  "io/ioutil"
  "fmt"
  "io"
  "os"
  "log"
  "errors"
  "strings"
  "strconv"
)

const action_sum string = "sum"
const hour int = 60*60
const day int = hour*24
const month int = day*31
const year int = month*12
const opt_pair = "PAIR"
const opt_date = "DATE"
const opt_time = "TIME"
const opt_open = "OPEN"
const opt_high = "HIGH"
const opt_low = "LOW"
const opt_close = "CLOSE"
const opt_vol = "VOL"
var label_order_pair = 0
var label_order_date = 1
var label_order_time = 2
var label_order_open = 3
var label_order_high = 4
var label_order_low = 5
var label_order_close = 6
var label_order_vol = 7

func main() {
  var command string = "search"
  var group_sec int = 60
  var join_separator string = ","
  var division_separator string = ","

  var fp *os.File
  var err error
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

  opts := getOptions().Options
  if opts.Join_separator != "" {
    join_separator = opts.Join_separator
  }
  if opts.Division_separator != "" {
    division_separator = opts.Division_separator
  }
  if opts.Order != nil {
    for i := 0; i < len(opts.Order); i++ {
      if opts.Order[i] != "" && opts.Order[i] == opt_pair {
        label_order_pair = i
      } else if opts.Order[i] != "" && strings.ToUpper(opts.Order[i]) == opt_date {
        label_order_date = i
      } else if opts.Order[i] != "" && strings.ToUpper(opts.Order[i]) == opt_time {
        label_order_time = i
      } else if opts.Order[i] != "" && strings.ToUpper(opts.Order[i]) == opt_open {
        label_order_open = i
      } else if opts.Order[i] != "" && strings.ToUpper(opts.Order[i]) == opt_high {
        label_order_high = i
      } else if opts.Order[i] != "" && strings.ToUpper(opts.Order[i]) == opt_low {
        label_order_low = i
      } else if opts.Order[i] != "" && strings.ToUpper(opts.Order[i]) == opt_close {
        label_order_close = i
      } else if opts.Order[i] != "" && strings.ToUpper(opts.Order[i]) == opt_vol {
        label_order_vol = i
      }
    }
  }


  // fp, err = os.Open(os.Args[1])
  // if err != nil  {
  //   failOnError(err)
  // }
  // defer fp.Close()

  // reader := csv.NewReader(fp)
  info, err := os.Stdin.Stat()
  if err != nil {
      failOnError(err)
  }
  if info.Mode() & os.ModeCharDevice != 0 || info.Size() <= 0 {
    failOnError(errors.New(fmt.Sprintf("", "device error!")))
  }
  fp = os.Stdin
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
      if(pattern == record[label_order_date][0:len(pattern)] && isNumber(record[label_order_date][0:4])) {
        div, unit := getNumberOfDivision(group_sec)
        datetime_str := record[label_order_date] + record[label_order_time]
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
      if(pattern == record[label_order_date][0:len(pattern)]) {
        fmt.Println(strings.Join(record, join_separator))
      }
    }
  }
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

type Options struct {
    Options Option `json:"options"`
}

type Option struct {
  Order []string `json:"order"`
  Division_separator   string `json:"division_separator"`
  Join_separator   string `json:"join_separator"`
}

func getOptions() Options {
  var options Options
  json_file, err := os.Open("opts.json");
  if err != nil {
    return options
  }
  defer json_file.Close()
  bytes, _ := ioutil.ReadAll(json_file)

  json.Unmarshal([]byte(bytes), &options)

  return options
}

func merge_data(record []string, data []string) (ret []string) {
  rPair := record[label_order_pair]
  rDate := record[label_order_date]
  rTime := record[label_order_time]
  rHigh := record[label_order_high]
  rLow := record[label_order_low]
  rClose := record[label_order_close]
  rVol:= record[label_order_vol]

  sOpen := data[label_order_open]
  sHigh := data[label_order_high]
  sLow := data[label_order_low]
  sVol:= data[label_order_vol]
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


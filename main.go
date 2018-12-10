/*

Simple FX Candle Converter

Usage

  ex:)
    prameter1 => filter
    prameter2 => unit

    ./sfcc 2018 $((60*60)) > usjp2018.txt

    OR

    cat sample.txt | ./sfcc > usjp2018.txt

  opts.json
    the file need to put working directory excute the job.

    input              => field order of a input file
    output             => field order of a output file
    division_separator => input's separator
    join_separator     => output's separator
    data_path          => datafile(csv, tsv...) if the field is empty, get data from stdin through pipe system.
    filter             => if '2018' is set, just excluded 2018 year's data.
    unit               => set seconds. 10min(10*60) => 600, 1hour(60*60) => 3600

    do not remove these keys in opts.json
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
  "regexp"
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
const opt_calc_dif = "CALC_DIF"
const opt_calc_accel = "CALC_ACCEL"
const minmum_group = 60
const unset = -1
var index_order_pair = 0
var index_order_date = 1
var index_order_time = 2
var index_order_open = 3
var index_order_high = 4
var index_order_low = 5
var index_order_close = 6
var index_order_vol = 7

func main() {
  var group_sec int = minmum_group
  var join_separator string = ","
  var division_separator string = ","
  var required_field_count int = 8
  var pattern string = ""
  var illegal_chars string = "[:|.]"
  var fp *os.File
  var err error

  opts := getOptions().Options
  if opts.Join_separator != "" {
    join_separator = opts.Join_separator
  }
  if opts.Division_separator != "" {
    division_separator = opts.Division_separator
  }
  if opts.Input != nil {
    required_field_count = 0
    index_order_pair = unset
    index_order_date = unset
    index_order_time = unset
    index_order_open = unset
    index_order_high = unset
    index_order_low = unset
    index_order_close = unset
    index_order_vol = unset
    for i := 0; i < len(opts.Input); i++ {
      if opts.Input[i] != "" && opts.Input[i] == opt_pair {
        index_order_pair = i
        required_field_count++
      } else if opts.Input[i] != "" && strings.ToUpper(opts.Input[i]) == opt_date {
        index_order_date = i
        required_field_count++
      } else if opts.Input[i] != "" && strings.ToUpper(opts.Input[i]) == opt_time {
        index_order_time = i
        required_field_count++
      } else if opts.Input[i] != "" && strings.ToUpper(opts.Input[i]) == opt_open {
        index_order_open = i
        required_field_count++
      } else if opts.Input[i] != "" && strings.ToUpper(opts.Input[i]) == opt_high {
        index_order_high = i
        required_field_count++
      } else if opts.Input[i] != "" && strings.ToUpper(opts.Input[i]) == opt_low {
        index_order_low = i
        required_field_count++
      } else if opts.Input[i] != "" && strings.ToUpper(opts.Input[i]) == opt_close {
        index_order_close = i
        required_field_count++
      } else if opts.Input[i] != "" && strings.ToUpper(opts.Input[i]) == opt_vol {
        index_order_vol = i
        required_field_count++
      }
    }
  }
  if opts.Filter != "" {
    pattern = opts.Filter
  }
  if opts.Illegal_chars != "" {
    illegal_chars = opts.Illegal_chars
  }
  rep := regexp.MustCompile(illegal_chars)
  group_sec = opts.Unit
  if len(os.Args) > 1 {
    pattern = os.Args[1]

    if len(os.Args) > 2 {
      if isNumber(os.Args[2]) {
        group_sec = toNumber(os.Args[2])
      }
    }
  }

  info, err := os.Stdin.Stat()
  if err != nil || info.Size() <= 0 {
    if opts.Data_path != "" {
      fp, err = os.Open(opts.Data_path)
      if err != nil  {
        failOnError(err)
      }
      defer fp.Close()
    }
  } else {
    if info.Mode() & os.ModeCharDevice != 0 {
      failOnError(errors.New(fmt.Sprintf("", "device error!")))
    }
    fp = os.Stdin
  }

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

    str_date := record[index_order_date]
    str_time := record[index_order_time]
    if group_sec >= minmum_group && len(record) >= required_field_count {
      if len(illegal_chars) > 0 {
        str_date = rep.ReplaceAllString(str_date, "")
        str_time = rep.ReplaceAllString(str_time, "")
        padding := 6 - len(str_time)
        if padding > 0 {
          str_time = str_time + strings.Repeat("0", padding)
        }
      }
      if(pattern == str_date[0:len(pattern)] && isNumber(str_date[0:4])) {
        div, unit := getNumberOfDivision(group_sec)
        datetime_str := str_date + str_time
        key := getKey(datetime_str, div, unit)
        if prevKey != "" && mergedDataDict[key] == nil {
          fmt.Println(strings.Join(get_output_record(mergedDataDict[prevKey], opts.Output, group_sec), join_separator))
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
      if(pattern == str_date[0:len(pattern)]) {
        fmt.Println(strings.Join(record, join_separator))
      }
    }
  }
  if prevKey != "" && mergedDataDict[prevKey] != nil {
    fmt.Println(strings.Join(get_output_record(mergedDataDict[prevKey], opts.Output, group_sec), join_separator))
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
  Input []string `json:"input"`
  Output []string `json:"output"`
  Division_separator string `json:"division_separator"`
  Join_separator string `json:"join_separator"`
  Data_path string `json:"data_path"`
  Filter string `json:"filter"`
  Unit int `json:"unit"`
  Illegal_chars string `json:"illegal_chars"`
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

func get_output_record(record []string, output []string, total int) (ret []string) {
  if output == nil {
    return record
  }
  result := make([]string, 0, 10)
  for _, v := range output {
    switch (v) {
    case opt_pair:
      if index_order_pair > unset {
        result = append(result, record[index_order_pair])
      }
    case opt_date:
      if index_order_date > unset {
        result = append(result, record[index_order_date])
      }
    case opt_time:
      if index_order_time > unset {
        result = append(result, record[index_order_time])
      }
    case opt_open:
      if index_order_open > unset {
        result = append(result, record[index_order_open])
      }
    case opt_high:
      if index_order_high > unset {
        result = append(result, record[index_order_high])
      }
    case opt_low:
      if index_order_low > unset {
        result = append(result, record[index_order_low])
      }
    case opt_close:
      if index_order_close > unset {
        result = append(result, record[index_order_close])
      }
    case opt_vol:
      if index_order_vol > unset {
        result = append(result, record[index_order_vol])
      }
    case opt_calc_dif:
      if(index_order_high > unset && index_order_low > unset) {
        dif := getDif(toFloat(record[index_order_high]), toFloat(record[index_order_low]))
        ret := fmt.Sprintf("%.5f", dif)
        result = append(result, ret)
      }
    case opt_calc_accel:
      if(index_order_open > unset && index_order_close > unset) {
        ac := getAccel(toFloat(record[index_order_open]), toFloat(record[index_order_close]), total)
        ret := fmt.Sprintf("%.7f", ac)
        result = append(result, ret)
      }
    default:

    }
  }
  return result
}

func getDif(heigh float64, low float64) (float64) {
  return heigh - low
}

func getAccel(open_price float64, close_price float64, total int) (float64) {
  return (close_price - open_price) / float64(total)
}

func merge_data(record []string, data []string) (ret []string) {
  if index_order_pair > unset {
    record[index_order_pair] = record[index_order_pair]
  }
  if index_order_date > unset {
    record[index_order_date] = data[index_order_date]
  }
  if index_order_time > unset {
    record[index_order_time] = data[index_order_time]
  }
  if index_order_open > unset {
    record[index_order_open] = data[index_order_open]
  }
  if index_order_high > unset {
    rHigh := record[index_order_high]
    sHigh := data[index_order_high]
    if sHigh > rHigh {
      rHigh = sHigh
    }
    record[index_order_high] = rHigh
  }
  if index_order_low > unset {
    rLow := record[index_order_low]
    sLow := data[index_order_low]
    if sLow < rLow {
      rLow = sLow
    }
    record[index_order_low] = rLow
  }
  if index_order_close > unset {
    record[index_order_close] = record[index_order_close]
  }
  if index_order_vol > unset {
    rVol:= record[index_order_vol]
    sVol:= data[index_order_vol]
    rVol = toString(toNumber(rVol) + toNumber(sVol))
    record[index_order_vol] = rVol
  }

  return record
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

func isFloat(val string) bool {
  if _, err := strconv.ParseFloat(val, 32); err == nil {
    return true
  }
  return false
}

func toFloat(val string) float64 {
  v, _ := strconv.ParseFloat(val, 32)
  return v
}

func toString(val int) string {
  return strconv.Itoa(val)
}

func breakpoint() {
  return
}


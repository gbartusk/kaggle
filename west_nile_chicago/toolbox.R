#
# - WNV Chicago - Toolboox
#

# - loader: spray data set
load_spray <- function(file_path)
{
    # - check argument
    if( missing(file_path) )
    {
        stop("argument \"file_path\" is missing, with no default")
    }
    
    # - required libraries
    require(readr)          # - fast csv reading
    require(dplyr)          # - data manipulation
    require(lubridate)      # - dates
    
    # - load data into memory with defined schema
    df_data <- 
        readr::read_csv(
            file_path, 
            col_types = list(
                Date = col_character(),
                Time = col_character(),
                Latitude = col_double(),
                Longitude = col_double()
            )
        ) %>%
        dplyr::mutate(
            # - lubridate fast_strptime doesnt support this format for time and 
            #   strptime returns POSIXlt which isnt supported in dplyr (#670)
            DateTime = as.POSIXct(strptime(paste(Date,Time), "%Y-%m-%d %I:%M:%S %p")),
            Date = as.Date(Date, "%Y-%m-%d"),
            DateStr = format(Date, "%Y%m%d"),
            Year = lubridate::year(Date),
            #Month = lubridate::month(Date, label=TRUE, abbr=TRUE)
            Month = as.factor(base::months(Date))
        )
    
    # - return data set
    invisible(df_data)
}


# - loader: weather data set
load_weather <- function(file_path)
{
    # - check argument
    if( missing(file_path) )
    {
        stop("argument \"file_path\" is missing, with no default")
    }
    
    # - required libraries
    require(dplyr)      # - data manipulation
    require(stringr)    # - string manipulation
    
    # - get weather station location data
    #   note data is in long format with repeated dates for each station
    station_loc <- get_weather_station_local()
    
    # - load data into memory
    df_data <- read.csv(unzip(file_path), colClasses = "character", 
        na.string=c(""," ","-","M"))
    
    # - strip white space
    #   the strip.white arg in read.csv wont work since white space is quoted
    df_data <- as.data.frame(lapply(df_data, stringr::str_trim), 
        stringsAsFactors=FALSE)
    
    # - update all trace readings (SnowFall and PrecipTotal)
    df_data[df_data=="T"] <- NA
    
    # - define schema
    df_data <- df_data %>%
        dplyr::mutate(
            Station = as.integer(Station),
            
            Latitude = ifelse(
                Station==1, station_loc[["s1"]]$lat, 
                ifelse(Station==2, station_loc[["s2"]]$lat, NA)
            ),
            Longitude = ifelse(
                Station==1, station_loc[["s1"]]$lon, 
                ifelse(Station==2, station_loc[["s2"]]$lon, NA)
            ),
            Elevation = ifelse(
                Station==1, station_loc[["s1"]]$elev, 
                ifelse(Station==2, station_loc[["s2"]]$elev, NA)
            ),
            
            Date = as.Date(Date),
            DateStr = format(Date, "%Y%m%d"),
            Year = lubridate::year(Date),
            #Month = lubridate::month(Date, label=TRUE, abbr=TRUE),
            Month = as.factor(base::months(Date)),
            
            Tmax = as.numeric(Tmax),
            Tmin = as.numeric(Tmin),
            Tavg = as.numeric(Tavg),
            
            Depart = as.numeric(Depart),
            
            # - temp at which the air can no longer hold all of the water 
            #   vapor which is mixed with it, always <= temp
            DewPoint = as.numeric(DewPoint),
            # - lowest temp that can be reached by evaporating water into air,
            #   always be <= the temp
            WetBulb = as.numeric(WetBulb),
            
            Heat = as.integer(Heat),
            Cool = as.integer(Cool),
            
            Sunrise = Sunrise,
            Sunset = Sunset,
            CodeSum = CodeSum,
            
            # - snow depth?
            Depth = as.numeric(Depth),
            # - all missing values
            Water1 = Water1,
            SnowFall = as.numeric(SnowFall),
            PrecipTotal = as.numeric(PrecipTotal),
            # - station pressure?
            StnPressure = as.numeric(StnPressure),
            SeaLevel = as.numeric(SeaLevel),
            # - resultant wind speed
            ResultSpeed = as.numeric(ResultSpeed),
            # - resultant direction
            ResultDir = as.numeric(ResultDir),
            
            AvgSpeed = as.numeric(AvgSpeed)
        )
    
    # - return data set
    invisible(df_data)
}


# - loader: training data set
load_train <- function(file_path)
{
    # - check argument
    if( missing(file_path) )
    {
        stop("argument \"file_path\" is missing, with no default")
    }
    
    # - required libraries
    require(readr)          # - fast csv reading
    require(dplyr)          # - data manipulation
    require(stringr)        # - string manipulation
    require(lubridate)      # - dates
    
    # - load data into memory with defined schema
    df_data <- 
        readr::read_csv(
            file_path,
            col_types = list(
                Date = col_date(),
                Address = col_character(),
                Species = col_character(),
                Block = col_integer(),
                Street = col_character(),
                Trap = col_character(),
                AddressNumberAndStreet = col_character(),
                Latitude = col_double(),
                Longitude = col_double(),
                AddressAccuracy = col_character(),
                NumMosquitos = col_integer(),
                # - 1: west nile present, 0: not present
                WnvPresent = col_integer()
            )
        ) %>%
        dplyr::mutate(
            # - not doing this in read_csv since need to specify levels
            Species = as.factor(Species),
            # - http://stackoverflow.com/questions/2859705/google-maps-api-geocoding-accuracy-chart
            AddressAccuracy = factor(AddressAccuracy, 
                levels=c(3,5,8,9), 
                labels=c("sub_region","post_code","address","premise")),
            WnvPresentF = factor(WnvPresent,levels=c(0,1),labels=c("absent","present")),
            DateStr = format(Date, "%Y%m%d"),
            Year = lubridate::year(Date),
            # - this creates an ordered factors which shows ploynomial values in regression
            #   http://stackoverflow.com/questions/10954167/r-regression-with-months-as-independent-variables-labels
            #Month = lubridate::month(Date, label=TRUE, abbr=TRUE),
            Month = as.factor(base::months(Date)),
            Day = lubridate::mday(Date),
            WkDay = lubridate::wday(Date, label=TRUE, abbr=TRUE),
            # - traps setup near an established trap to enhance surveillance
            SatelliteTrap = 1 * !grepl("[0-9]", stringr::str_sub(Trap,-1))
        )
    
    # - closest weather station (not vectorized, very slow)
    df_data$Station = mapply(get_closest_station, df_data$Longitude, df_data$Latitude)
    
    # - return data set
    invisible(df_data)
}


# - loader: test data set
load_test <- function(file_path)
{
    # - check argument
    if( missing(file_path) )
    {
        stop("argument \"file_path\" is missing, with no default")
    }
    
    # - required libraries
    require(readr)          # - fast csv reading
    require(dplyr)          # - data manipulation
    require(stringr)        # - string manipulation
    require(lubridate)      # - dates
    
    # - load data into memory with defined schema
    df_data <- 
        readr::read_csv(
            file_path,
            col_types = list(
                Id = col_integer(),
                Date = col_date(),
                Address = col_character(),
                Species = col_character(),
                Block = col_integer(),
                Street = col_character(),
                Trap = col_character(),
                AddressNumberAndStreet = col_character(),
                Latitude = col_double(),
                Longitude = col_double(),
                AddressAccuracy = col_character()
            )
        ) %>%
        dplyr::mutate(
            # - prep the species column by moving the test-only UNSPECIFIED 
            #   CULEX to CULEX ERRATICUS, and re-doing the levels logistic 
            #   regression will complain otherwise
            # - this logic is copied from kaggle script, not sure why they choose this species
            #   https://www.kaggle.com/users/48625/mlandry/predict-west-nile-virus/h2o-starter
            Species = as.factor(ifelse(Species=="UNSPECIFIED CULEX", "CULEX ERRATICUS", Species)),
            
            # - http://stackoverflow.com/questions/2859705/google-maps-api-geocoding-accuracy-chart
            AddressAccuracy = factor(AddressAccuracy, 
                levels=c(3,5,8,9), 
                labels=c("sub_region","post_code","address","premise")),
            
            DateStr = format(Date, "%Y%m%d"),
            Year = lubridate::year(Date),
            #Month = lubridate::month(Date, label=TRUE, abbr=TRUE),
            Month = as.factor(base::months(Date)),
            Day = lubridate::mday(Date),
            WkDay = lubridate::wday(Date, label=TRUE, abbr=TRUE)
        )
    
    # - closest weather station (not vectorized, very slow)
    df_data$Station = mapply(get_closest_station, df_data$Longitude, df_data$Latitude)
    
    # - return data set
    invisible(df_data)
}


# - loader: submission
load_submission <- function(file_path)
{
    # - check argument
    if( missing(file_path) )
    {
        stop("argument \"file_path\" is missing, with no default")
    }
    
    # - required libraries
    require(readr)          # - fast csv reading
    require(dplyr)          # - data manipulation
    
    # - load data into memory with defined schema
    df_data <- 
        readr::read_csv(
            file_path,
            col_types = list(
                Id = col_integer(),
                WnvPresent = col_integer()
            )
        )
    
    # - return data set
    invisible(df_data)
}


# - get: hardcoded weather station location
get_weather_station_local <- function()
{
    require(ggmap)      # - find lat/lon for weather stations
    
    # - weather data is split between two stations which are listed
    #   on kaggle data description page (hardcoding here)
    #stat1_name <- "CHICAGO O'HARE INTERNATIONAL AIRPORT"
    #stat1_loc <- ggmap::geocode(stat1_name, messaging=FALSE)
    #stat2_name <- "CHICAGO MIDWAY INTL ARPT"
    #stat2_loc <- ggmap::geocode(stat2_name, messaging=FALSE)
    
    # - find slighly different lat/lon for o'hare using the above, hence,
    #   simply hardcoding the values from the kaggle data page
    station_loc <- list(
        "s1" = list("lat"=41.995, "lon"=-87.933, "elev"=662),
        "s2" = list("lat"=41.786, "lon"=-87.752, "elev"=612)
    )
    
    # - return
    invisible(station_loc)
}


# - get: closest weather station
get_closest_station <- function(lon, lat)
{
    # - check arguments
    if( missing(lon) || !is.numeric(lon) )
    {
        stop("argument \"lon\" is missing or not numeric")
    }
    if( missing(lat) || !is.numeric(lat) )
    {
        stop("argument \"lat\" is missing or not numeric")
    }
    
    require(geosphere)       # - spherical distance
    
    # - get weather station location data
    station_loc <- get_weather_station_local()
    
    # - euclidian distances are not accurate since on a sphere
    #   using haverstine formula (defaults to earth radius)
    station1_dist <- geosphere::distHaversine(
        c(station_loc[["s1"]]$lon, station_loc[["s1"]]$lat),
        c(lon, lat)
    )
    station2_dist <- geosphere::distHaversine(
        c(station_loc[["s2"]]$lon, station_loc[["s2"]]$lat),
        c(lon, lat)
    )
    
    # - select the closest distance
    if (station1_dist < station2_dist)
    {
        return(1)
    }
    else if (station1_dist >= station2_dist)
    {
        return(2)
    }
    else
    {
        # - should never be the case but just in case
        return(NA)
    }
}


# - create submission csv file
create_submission <- function(file_path_out, model_fit)
{
    # - load: sample submission
    df_submission <- load_submission(path_data_submission)
    
    # - predict
    #   this assumes df_test_wthr is already loaded in memory!!!
    df_submission$WnvPresent <- predict(model_fit, newdata=df_test_wthr, type="response")
    
    # - write submission file
    readr::write_csv(df_submission, file_path_out)
    
    return(TRUE)
}


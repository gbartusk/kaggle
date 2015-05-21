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
            ),
            progress = FALSE
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
load_weather <- function(file_path, impute=TRUE)
{
    # - check argument
    if( missing(file_path) )
    {
        stop("argument \"file_path\" is missing, with no default")
    }
    
    # - required libraries
    require(dplyr)      # - data manipulation
    require(stringr)    # - string manipulation
    
    # - get weather station location data (hardcoded location from instructions)
    #   note: weather data is in long format with repeated dates for each station
    station_loc <- get_weather_station_local()
    
    # - debugging
    #file_path <- path_data_weather
    
    # - load data into memory
    df_data <- read.csv(unzip(file_path), colClasses = "character", 
        na.string=c(""," ","-","M"))
    
    # - strip white space
    #   the strip.white arg in read.csv wont work since white space is quoted
    df_data <- as.data.frame(lapply(df_data, stringr::str_trim), 
        stringsAsFactors=FALSE)
    
    # - update all trace readings (SnowFall and PrecipTotal)
    df_data[df_data=="T"] <- NA
    df_data[df_data=="M"] <- NA
    
    # - define schema
    df_data <- df_data %>%
        dplyr::mutate(
            # - dates
            Date = as.Date(Date),
            DateStr = format(Date, "%Y%m%d"),
            Year = lubridate::year(Date),
            #Month = lubridate::month(Date, label=TRUE, abbr=TRUE),
            Month = as.factor(base::months(Date)),
            
            # - weather station
            Station = as.integer(Station),
            # - flip station for imputation join
            StationFlip = ifelse(Station==1,2,1),
            
            # - location
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
            
            # - time when sun rises and sets in the day
            Sunrise = Sunrise,
            #Sunrise = as.POSIXct(strptime(paste(DateStr,Sunrise), "%Y%m%d %H%M")),
            # - sunset has some instances where min is 60 (range is 0-59)
            SunsetMin = as.numeric(stringr::str_sub(Sunset,-2,-1)),
            SunsetHr = as.numeric(stringr::str_sub(Sunset,1,2)),
            Sunset = ifelse(
                SunsetMin==60 && SunsetHr<23,
                paste(sprintf("%02d",SunsetHr+1), "00", sep=""),
                ifelse(SunsetMin==60 && SunsetHr==23, "2359", Sunset)),
            #Sunset = as.POSIXct(strptime(paste(DateStr,Sunset), "%Y%m%d %H%M")),            
            
            # - temperature
            Tmax = as.numeric(Tmax),
            Tmin = as.numeric(Tmin),
            Tavg = as.numeric(Tavg),
            # - departure from normal (for temp?)
            Depart = as.numeric(Depart),
            
            # - temp at which the air can no longer hold all of the water 
            #   vapor which is mixed with it, always <= temp
            # - dew point is a messure of relative humidity, at 100% the dew point
            #   = the temp and air is max saturated, this causes it to feel very 
            #   how since sweat will not evaporate and hence no cool effect
            # - at low dew points, the air is dry and cause skin to crack
            DewPoint = as.numeric(DewPoint),
            # - lowest temp that can be reached by evaporating water into air,
            #   always be <= the temp.
            # - this is very similar to DewPoint (97% correlation)
            WetBulb = as.numeric(WetBulb),
            
            # - https://www.kaggle.com/c/predict-west-nile-virus/forums/t/14186/what-is-hot-day/78288#post78288
            # - heat is this: http://en.wikipedia.org/wiki/Heating_degree_day
            #   demand of energy needed to heat or cool a building, DERIVED from temp
            # - aka as HDD=heating degree day and CDD
            Heat = as.integer(Heat),
            Cool = as.integer(Cool),
            
            # - weather phenoena
            CodeSum = CodeSum,
            
            # - snow depth?
            Depth = as.numeric(Depth),
            # - all missing values
            Water1 = Water1,
            SnowFall = as.numeric(SnowFall),
            
            # - rain in 24h period (inches)
            PrecipTotal = as.numeric(PrecipTotal),
            
            # - station pressure?
            StnPressure = as.numeric(StnPressure),
            SeaLevel = as.numeric(SeaLevel),
            # - resultant wind speed
            ResultSpeed = as.numeric(ResultSpeed),
            # - resultant direction
            ResultDir = as.numeric(ResultDir),
            # - 
            AvgSpeed = as.numeric(AvgSpeed)
        ) %>%
        dplyr::select(
            # - all values are na
            -Water1,
            # - helper variables
            -SunsetMin, -SunsetHr
        )
    
    # - impute missing values
    if (impute==TRUE)
    {
        # - impute: create data that can be joined on itself using flipped station
        df_data_flip <- df_data %>%
            dplyr::select(
                DateStr, StationFlip, Tavg, Heat, Cool, WetBulb, StnPressure,
                SeaLevel, AvgSpeed, PrecipTotal, Sunrise, Sunset, Depart, Depth,
                SnowFall, CodeSum
            ) %>%
            dplyr::rename(
                TavgFlip=Tavg, 
                HeatFlip=Heat, 
                CoolFlip=Cool, 
                WetBulbFlip=WetBulb,
                StnPressureFlip=StnPressure,
                SeaLevelFlip=SeaLevel,
                AvgSpeedFlip=AvgSpeed,
                PrecipTotalFlip=PrecipTotal,
                SunriseFlip=Sunrise, 
                SunsetFlip=Sunset,
                DepartFlip=Depart,
                DepthFlip=Depth,
                SnowFallFlip=SnowFall,
                CodeSumFlip=CodeSum
            )
        
        # - impute: data based on other weather station
        df_data2 <- df_data %>%
            dplyr::arrange(Station, DateStr) %>%
            dplyr::left_join(
                df_data_flip,
                by=c("DateStr", "Station"="StationFlip")
            ) %>%
            dplyr::mutate(
                Tavg = ifelse(is.na(Tavg), TavgFlip, Tavg),
                Heat = ifelse(is.na(Heat), HeatFlip, Heat),
                Cool = ifelse(is.na(Cool), CoolFlip, Cool),
                WetBulb = ifelse(is.na(WetBulb), WetBulbFlip, WetBulb),
                StnPressure = ifelse(is.na(StnPressure), StnPressureFlip, StnPressure),
                SeaLevel = ifelse(is.na(SeaLevel), SeaLevelFlip, SeaLevel),
                AvgSpeed = ifelse(is.na(AvgSpeed), AvgSpeedFlip, AvgSpeed),
                PrecipTotal = ifelse(is.na(PrecipTotal), PrecipTotalFlip, PrecipTotal),
                Sunrise = ifelse(is.na(Sunrise), SunriseFlip, Sunrise),
                Sunset = ifelse(is.na(Sunset), SunsetFlip, Sunset),
                Depart = ifelse(is.na(Depart), DepartFlip, Depart),
                Depth = ifelse(is.na(Depth), DepthFlip, Depth),
                SnowFall = ifelse(is.na(SnowFall), SnowFallFlip, SnowFall),
                CodeSum = ifelse(is.na(CodeSum), CodeSumFlip, CodeSum)
            ) %>%
            dplyr::select(
                -TavgFlip, -HeatFlip, -CoolFlip, -WetBulbFlip, -StnPressureFlip,
                -SeaLevelFlip, -AvgSpeedFlip, -PrecipTotalFlip, -SunriseFlip,
                -SunsetFlip, -DepartFlip, -DepthFlip, -SnowFallFlip, -CodeSumFlip
            )
        
        # - impute: data based on 1d lookback
        df_data3 <- df_data2 %>%
            dplyr::group_by(Station) %>%
            dplyr::mutate(
                StnPressure = ifelse(is.na(StnPressure), lag(StnPressure,1), StnPressure),
                # - two values in a row are na
                SnowFall = ifelse(is.na(SnowFall), lag(SnowFall,1), SnowFall),
                SnowFall = ifelse(is.na(SnowFall), lag(SnowFall,1), SnowFall),
                PrecipTotal = ifelse(is.na(PrecipTotal), lag(PrecipTotal,1), PrecipTotal),
                PrecipTotal = ifelse(is.na(PrecipTotal), lag(PrecipTotal,1), PrecipTotal),
                CodeSum = ifelse(is.na(CodeSum), lag(CodeSum,1), CodeSum)
            ) %>%
            dplyr::ungroup()
        
        # - CodeSum Still have 872 missing values (30%)
        #   removing since i dont see a current use for it
        df_data3 <- df_data3 %>% dplyr::select(-CodeSum)
        
        # -  columns with NAs
        #before <- sapply(df_data, function(x) sum(is.na(x)))
        #after2 <- sapply(df_data2, function(x) sum(is.na(x)))
        #after3 <- sapply(df_data3, function(x) sum(is.na(x)))
        #rbind(before, after2, after3)
        
        # - debug
        #sum(sapply(df_data, function(x) sum(is.na(x))))
        #sum(sapply(df_data2, function(x) sum(is.na(x))))
        #df_data3[is.na(df_data3$SnowFall),]
        #dplyr::filter(df_data3, DateStr %in% c("20081026","20081027","20081028"))
        #View(df_data2[is.na(df_data2$CodeSum),])
        
        # - PrecipTotal has very large outliers, take log and add small amount
        #   due to zero values (.01 is the smallest value >0)
        #table(Hmisc::cut2(df_train_wthr$PrecipTotal,cuts=c(0,.1,.5,1,1.5,2,3.9)))
        #range(df_weather$PrecipTotal[df_weather$PrecipTotal>0])
        df_data3$PrecipTotalLog <- log(df_data3$PrecipTotal+.001)
        
        # - for return
        df_data <- as.data.frame(df_data3)
        
        # - clean up
        rm(df_data2, df_data3)
    }
    
    # - update sunrise and sunset to dateTimes
    #   the imputation was causing them to become numeric (not sure why yet)
    df_data <- df_data %>% 
        dplyr::mutate(
            # - sunrise
            Sunrise = as.POSIXct(strptime(paste(DateStr,Sunrise), "%Y%m%d %H%M")),
            SunriseNum = as.numeric(format(Sunrise, "%H.%M")),
            # - sunset
            Sunset = as.POSIXct(strptime(paste(DateStr,Sunset), "%Y%m%d %H%M")),
            SunsetNum = as.numeric(format(Sunset, "%H.%M"))
        )
    
    # - return data set
    invisible(df_data)
}


# - loader: training data set
load_train <- function(file_path, collapse=TRUE)
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
            ),
            progress = FALSE
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
            # - this is creating ordered factors too!
            #WkDay = lubridate::wday(Date, label=TRUE, abbr=TRUE),
            WkDay = as.factor(base::weekdays(Date)),
            # - traps setup near an established trap to enhance surveillance
            SatelliteTrap = 1 * !grepl("[0-9]", stringr::str_sub(Trap,-1))
        )
    
    # - closest weather station to trap location (not vectorized, very slow)
    df_data$Station = mapply(get_closest_station, df_data$Longitude, df_data$Latitude)
    
    # - no missing values in training data that need to be imputed
    #dim(na.omit(df_data)) == dim(df_data)
    
    # - collapsing data to distinct data / location / trap (move to udf!)
    if (collapse==TRUE)
    {
        # - the data is organized so when the number of mosquitos >50 they split
        #   into another record/row in the data. 
        # - NumMosquitos is the number of mosquitoes caught in single trap
        # - training data: dim:(10506x18) -> dim:(8610x18)
        
        df_data <- as.data.frame(
            df_data %>%
            dplyr::group_by(
                Date,
                Latitude, Longitude, Station,
                Species, WnvPresent, WnvPresentF,
                Address, Block, Street, AddressNumberAndStreet, AddressAccuracy, 
                Trap, SatelliteTrap,
                DateStr, Year, Month, Day, WkDay
            ) %>%
            dplyr::summarise(
                NumMosquitos = sum(NumMosquitos, na.rm=T)
            ))
    }
    
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
    
    # - note that NumMosquitos is NOT included in the test set as it
    #   is considered part of the test result
    
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
            ),
            progress = FALSE
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
            #WkDay = lubridate::wday(Date, label=TRUE, abbr=TRUE)
            WkDay = as.factor(base::weekdays(Date))
        )
    
    # - closest weather station to trap location (not vectorized, very slow)
    df_data$Station = mapply(get_closest_station, df_data$Longitude, df_data$Latitude)
    
    # - no missing values in training data that need to be imputed
    #dim(na.omit(df_data)) == dim(df_data)
    
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
    

# - create submission csv file (**under construction**)
create_submission <- function(file_path_out, model_fit)
{
    if( missing(file_path_out) )
    {
        stop("argument \"file_path_out\" is missing")
    }
    if( missing(model_fit) )
    {
        stop("argument \"model_fit\" is missing")
    }
    
    # - required libraries
    require(dplyr)          # - data manipulation
    
    # - load: sample submission
    df_submission <- load_submission(file_path_out)
    
    # - predict
    #   this assumes df_test_wthr is already loaded in memory!!!
    df_submission$WnvPresent <- predict(model_fit, newdata=df_test_wthr, type="response")
    
    # - write submission file
    readr::write_csv(df_submission, file_path_out)
    
    return(TRUE)
}


# - join train and weather data sets
join_train_weather <- function()
{
    # - relying on data to already be in memory until i figure out how to
    #   get around the no pass by reference
    if( !exists("df_train") || !exists("df_weather") )
    {
        stop("df_train and df_weather must already exist in memory!")
    }
    
    # - required libraries
    require(dplyr)          # - data manipulation
    
    # - join data based on closest station
    df_train_wthr <- as.data.frame(
        df_train %>%
        dplyr::left_join(
            dplyr::select(df_weather, -Date,-Year,-Month,-Latitude,-Longitude), 
            by=c("DateStr","Station")
        ))
    
    # - return
    invisible(df_train_wthr)
}


# - join test and weather data sets
join_test_weather <- function()
{
    # - relying on data to already be in memory until i figure out how to
    #   get around the no pass by reference
    if( !exists("df_test") || !exists("df_weather") )
    {
        stop("df_test and df_weather must already exist in memory!")
    }
    
    # - required libraries
    require(dplyr)          # - data manipulation
    
    # - join data based on closest station
    df_test_wthr <- as.data.frame(
        df_test %>%
        dplyr::left_join(
            dplyr::select(df_weather, -Date,-Year,-Month,-Latitude,-Longitude), 
            by=c("DateStr","Station")
        ))
    
    # - return
    invisible(df_test_wthr)
}


# - correlation pairs
top_cor_pairs <- function(df, thresh)
{
    if( missing(df) )
    {
        stop("argument \"df\" is missing, with no default")
    }
    if( missing(thresh) )
    {
        stop("argument \"thresh\" is missing, with no default")
    }
    if ( thresh > 1 || thresh < 0 )
    {
        stop(paste0("argument \"thresh\" (",thresh,") must be in range [0,1]"))
    }
    
    require(dplyr)
    
    # - construct correlation matrix and set diagonal (cor=1) to zero
    corm <- cor(df)
    diag(corm) <- 0
    
    # - filter for any pair greater than threshold
    corm_filter <- which(abs(corm) > thresh, arr.ind=TRUE)
    
    # - create a data frame that indexes into the corr matrix
    df_top_cor <- data.frame(
        row=corm_filter[,"row"], 
        row_name=rownames(corm_filter), 
        col=corm_filter[,"col"], 
        col_name=colnames(corm)[as.vector(corm_filter[,"col"])]
    )
    
    # - create a key to allow us to filter distinct
    #   ie we need to remove all the duplicated: cor(a,b) = cor(b,a)
    df_top_cor$key = apply(
        df_top_cor[,c("row","col")],
        1, 
        function(m) 
            ifelse(m['row']<m['col'], paste(m['row'],m['col'],sep="_"),
            paste(m['col'],m['row'],sep="_")))
    
    # - filter distinct correlations
    df_top_cor <- dplyr::distinct(df_top_cor, key)
    
    # - insert correlation values into dataframe
    df_top_cor$cor = apply(
        df_top_cor[,c("row","col")],
        1, 
        function(m) round(corm[m['row'],m['col']]*100,2))
    
    # - clean up columns and sort
    df_top_cor <- df_top_cor %>% 
        dplyr::select(-key,-row,-col) %>%
        dplyr::arrange(desc(cor))
    
    # - return
    invisible(df_top_cor)
}


# - add pre-defined weather based PCA variables
get_weather_pcas <- function(df, temp=TRUE, pres_lvl=TRUE, speed=TRUE)
{
    # - temp: Tmax, Tmin, Tavg, DewPoint, WetBulb
    # - pres_lvl: StnPressure, SeaLevel
    # - speed: ResultSpeed, AvgSpeed
    
    if( missing(df) )
    {
        stop("argument \"df\" is missing, with no default")
    }
    
    require(caret)
    require(dplyr)
    
    # - pca: temp
    if ( temp == TRUE )
    {
        # - 1st PC:91%, 2nd PC: 97%
        df_train_sub <- dplyr::select(df, Tmax, Tmin, Tavg, DewPoint, WetBulb)
        pca_sub_obj <- caret::preProcess(df_train_sub, 
            pcaComp=2, method=c("BoxCox", "center", "scale", "pca"))
        pred_pca_sub <- predict(pca_sub_obj, df_train_sub)
        df$PC1_temp <- pred_pca_sub$PC1
        df$PC2_temp <- pred_pca_sub$PC2
        rm(df_train_sub, pca_sub_obj, pred_pca_sub)
    }
    
    # - pca: pressure and sea level
    if ( pres_lvl == TRUE )
    {
        # - 1st PC: 98%
        df_train_sub <- dplyr::select(df, StnPressure, SeaLevel)
        pca_sub_obj <- caret::preProcess(df_train_sub, 
            pcaComp=1, method=c("BoxCox", "center", "scale", "pca"))
        pred_pca_sub <- predict(pca_sub_obj, df_train_sub)
        df$PC1_pres_lvl <- pred_pca_sub[,1]
        rm(df_train_sub, pca_sub_obj, pred_pca_sub)
    }
    
    # - pca: speed
    if ( speed == TRUE )
    {
        # - 1st PC: 95%
        df_train_sub <- dplyr::select(df, ResultSpeed, AvgSpeed)
        pca_sub_obj <- caret::preProcess(df_train_sub, 
            pcaComp=1, method=c("BoxCox", "center", "scale", "pca"))
        pred_pca_sub <- predict(pca_sub_obj, df_train_sub)
        df$PC1_speed <- pred_pca_sub[,1]
        rm(df_train_sub, pca_sub_obj, pred_pca_sub)
    }
    
    # - return
    invisible(df)
}





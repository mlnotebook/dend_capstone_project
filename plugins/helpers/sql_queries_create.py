class SqlQueriesCreate:
    staging_immigration_create = ("""
        CREATE TABLE IF NOT EXISTS public.staging_immigration (
            cicid FLOAT PRIMARY KEY,
            i94yr FLOAT SORTKEY,
            i94mon FLOAT DISTKEY,
            i94cit FLOAT REFERENCES public.dim_i94cit(code),
            i94res FLOAT REFERENCES public.dim_i94cit(code),
            i94port CHAR(3) REFERENCES public.dim_i94port(code),
            arrdate FLOAT,
            i94mode FLOAT REFERENCES public.dim_i94mode(code),
            i94addr VARCHAR REFERENCES public.dim_i94addr(code),
            depdate FLOAT,
            i94bir FLOAT,
            i94visa FLOAT REFERENCES public.dim_i94visa(code),
            count FLOAT,
            dtadfile VARCHAR,
            visapost CHAR(3),
            occup CHAR(3),
            entdepa CHAR(1),
            entdepd CHAR(1),
            entdepu CHAR(1),
            matflag CHAR(1),
            biryear FLOAT,
            dtaddto VARCHAR,
            gender CHAR(1),
            insnum VARCHAR,
            airline VARCHAR,
            admnum FLOAT,
            fltno VARCHAR,
            visatype VARCHAR
        );
    """)

    staging_temperature_create = ("""
        CREATE TABLE IF NOT EXISTS public.staging_temperature (
            dt DATE,
            AverageTemperature FLOAT,
            AverageTemperatureUncertainty FLOAT,
            City VARCHAR,
            Country VARCHAR,
            Latitude VARCHAR,
            Longitude VARCHAR
        );
    """)
    
    staging_demographics_create = ("""
        CREATE TABLE IF NOT EXISTS public.staging_demographics (
            city VARCHAR,
            state VARCHAR,
            median_age FLOAT,
            male_population INT,
            female_population INT,
            total_population INT,
            number_of_veterans INT,
            foreign_born INT,
            average_household_size FLOAT,
            state_code CHAR(2) REFERENCES public.dim_i94addr(code),
            race VARCHAR,
            count INT
        )
        DISTSTYLE ALL;
    """)
        
    staging_airports_create = ("""
        CREATE TABLE IF NOT EXISTS public.staging_airports (
            ident VARCHAR,
            type VARCHAR,
            name VARCHAR,
            elevation_ft FLOAT,
            continent VARCHAR,
            iso_country VARCHAR,
            iso_region VARCHAR,
            municipality VARCHAR,
            gps_code VARCHAR,
            iata_code VARCHAR,
            local_code VARCHAR,
            coordinates VARCHAR
        );
    """)
    
    dim_i94cit_create = ("""
        CREATE TABLE IF NOT EXISTS public.dim_i94cit (
            code FLOAT PRIMARY KEY,
            country VARCHAR
        )
        DISTSTYLE ALL
;
    """
)
                     
    dim_i94port_create = ("""
        CREATE TABLE IF NOT EXISTS public.dim_i94port (
            code CHAR(3) PRIMARY KEY,
            port VARCHAR
        )
        DISTSTYLE ALL
    """)

    dim_i94mode_create = ("""
        CREATE TABLE IF NOT EXISTS public.dim_i94mode (
            code FLOAT PRIMARY KEY,
            mode VARCHAR
        )
        DISTSTYLE ALL
    """)

    dim_i94addr_create = ("""
        CREATE TABLE IF NOT EXISTS public.dim_i94addr (
            code CHAR(2) PRIMARY KEY,
            addr VARCHAR
        )
        DISTSTYLE ALL
    """)

    dim_i94visa_create = ("""
        CREATE TABLE IF NOT EXISTS public.dim_i94visa (
            code FLOAT PRIMARY KEY,
            type VARCHAR
        )
        DISTSTYLE ALL
    """)

    staging_tables = {'immigration': staging_immigration_create,
                      'demographics': staging_demographics_create,
                      'airport': staging_airports_create,
                      'temperature': staging_temperature_create}
    
    dim_tables = {'i94cit': dim_i94cit_create,
                  'i94port': dim_i94port_create,
                  'i94mode': dim_i94mode_create,
                  'i94addr': dim_i94addr_create,
                  'i94visa': dim_i94visa_create}


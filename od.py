"""
__author__ = Biswajit Saha

this multiprocessing script takes origin points e.g. meshblock centroid, gnaf point , destination points e.g. GTFS stops in a centre , GTFS data as inputs.
It generates orgin destination matrix in csv format for every miutes  for a specified period using GTFS timetable data.
"""



import arcpy
from arcpy import env
import os
import pandas as pd
import datetime
from itertools import product
from functools import partial
import multiprocessing
import logging
import time
import sys
import uuid


arcpy.CheckOutExtension('network')

def minutes_taken(t1,t2):
    return "minutes taken {}".format((t2-t1)/60.0)


def create_logger():
    # logger = multiprocessing.get_logger()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
 
    fh = logging.FileHandler("matrix.log")
    fh.setLevel(logging.INFO)
 
    fmt = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(fmt)
    fh.setFormatter(formatter)
 
    logger.addHandler(fh)
    #adding console handler
    consolehandler = logging.StreamHandler()
    consolehandler.setLevel(logging.DEBUG)
    consolehandler.setFormatter(formatter)
    logger.addHandler(consolehandler)
    return logger

logger = create_logger()



class Chunks(object):

    def __init__(self,fc):
        self.fc = fc
        self.desc = arcpy.Describe(self.fc)

    def by_size(self,n):
        sql_min = self._sql("min")
        sql_max = self._sql("max")
        cur_min = arcpy.da.SearchCursor(self.fc,["OID@"], sql_min)
        cur_max = arcpy.da.SearchCursor(self.fc,["OID@"], sql_max)
        minoid = cur_min.next()[0]
        maxoid = cur_max.next()[0]
        del sql_max, sql_min, cur_max, cur_min
        breaks = list(range(minoid, maxoid))[0:-1:n]
        breaks.append(maxoid +1)
        return [[breaks[b],breaks[b+1]]for b in range(len(breaks)-1)]
   
    def _sql(self, aggfunc):
        self.oidname = arcpy.AddFieldDelimiters(self.fc,self.desc.OIDFieldName)
        
        sql = '{} = (select {}({}) from {})'.format(self.oidname,aggfunc,self.oidname, os.path.basename(self.fc))
        return sql
    

class Locations:
    oidFieldName =None
    
    def __init__(self,fcMeshblocks,fcStops,params):
        self.fcMeshblocks = fcMeshblocks
        self.fcStops = fcStops
        self.params = params
        if not self.__class__.oidFieldName:
            self.__class__.oidFieldName= arcpy.Describe(self.fcMeshblocks).OIDFieldName
        self.prepare()
    
    def prepare(self):
        params= self.params
        #print(param1)
        oidfieldname =self.__class__.oidFieldName
        self.lyrStops = arcpy.MakeFeatureLayer_management(self.fcStops,'stopsLayer_{}_{}'.format(*params)).getOutput(0)
        self.lyrMeshblocks = arcpy.MakeFeatureLayer_management(self.fcMeshblocks,'meshblockLayer_{}_{}'.format(*params)," \"{}\" >={} AND \"{}\" < {}".format(oidfieldname,params[0],oidfieldname, params[1])).getOutput(0)
    

def result_to_df(result,MatrixOutputDataType,fields):
    cursor = result.searchCursor(MatrixOutputDataType, fields)
    df = pd.DataFrame(cursor)
    df.columns = fields
    return df
    


def calculate_matrix(fc_meshblocks,fc_stops,nds,date_range,default_cutoff,output_matrix_dir,params):
    ts =time.time()
    csvfilename=os.path.join(output_matrix_dir,"matrix_meshblocks_{}_{}.csv".format(*params))
    if os.path.exists(csvfilename):
        logger.info(f"{csvfilename} already exists")
        return False
    locations = Locations(fc_meshblocks,fc_stops,params)
    nd_layer_name = f"TransitNetwork_ND_{uuid.uuid4().hex}"
    input_origins = locations.lyrMeshblocks
    input_destinations = locations.lyrStops
    date_start,date_end = date_range
    
    arcpy.nax.MakeNetworkDatasetLayer(nds, nd_layer_name)
    nd_travel_modes = arcpy.nax.GetTravelModes(nd_layer_name)
    travel_mode = nd_travel_modes["Public transit time"]
    search_tolerance = 500.0#"500 Meters"

    # Instantiate a ClosestFacility solver object
    odcm = arcpy.nax.OriginDestinationCostMatrix(nds)
    #fieldmappings
    field_mappings_origins = odcm.fieldMappings(arcpy.nax.OriginDestinationCostMatrixInputDataType.Origins, True)
    field_mappings_origins["Name"].mappedFieldName = "meshblock_id"
    field_mappings_destinations = odcm.fieldMappings(arcpy.nax.OriginDestinationCostMatrixInputDataType.Destinations, True)
    field_mappings_destinations["Name"].mappedFieldName = "centrename"
    field_mappings_destinations["SourceOID"].mappedFieldName = "stop_id"


    # Set properties
    odcm.travelMode = travel_mode
    odcm.searchTolerance = search_tolerance
    #odcm.timeUnits = arcpy.nax.TimeUnits.Minutes
    odcm.defaultImpedanceCutoff =default_cutoff

    odcm.lineShapeType = arcpy.nax.LineShapeType.NoLine
    # # Load inputs
    logger.debug(f"loading inputs for {input_origins.name}")
    odcm.load(arcpy.nax.OriginDestinationCostMatrixInputDataType.Origins, input_origins,field_mappings_origins)
    logger.debug(f"loading inputs for {input_destinations.name}")
    odcm.load(arcpy.nax.OriginDestinationCostMatrixInputDataType.Destinations, input_destinations,field_mappings_destinations)
    logger.debug('loaded all inputs..')
  

    daterange_series=pd.date_range(date_start,date_end,freq='1min').to_series()
    dt_format = "%Y-%m-%d %H:%M:%S"
    dfs=[]
    for timeofday in daterange_series:
        t1 =time.time()
        timeofday = timeofday.to_pydatetime()
        #import pdb; pdb.set_trace()
       
        odcm.timeOfDay = timeofday#datetime.datetime(2019,11,4,8,10)
        # # Solve the analysis
        result = odcm.solve()
        # fieldnames = result.fieldNames(arcpy.nax.OriginDestinationCostMatrixOutputDataType.Lines)
        # df= result_to_df(result,arcpy.nax.OriginDestinationCostMatrixOutputDataType.Lines,fieldnames)
        # # Export the results to a feature class
        if result.solveSucceeded:
                      
            df_out_lines = result_to_df(result,arcpy.nax.OriginDestinationCostMatrixOutputDataType.Lines,result.fieldNames(arcpy.nax.OriginDestinationCostMatrixOutputDataType.Lines))

            
            t2 =time.time()
            logger.debug(f'solved for {timeofday.strftime(dt_format)} for {input_origins.name} and {input_destinations.name} and {minutes_taken(t1,t2)}')
            #print(df_combine.head(10))
            df_out_lines.rename(columns={'DestinationName':'centre_name','Total_Time':'total_minutes','OriginName':'meshblock_id','DestinationOID':'stop_id'},inplace =True)
            df_out_lines['time_of_day']=timeofday
            df_out_lines=df_out_lines[['meshblock_id','centre_name','stop_id','time_of_day','total_minutes']]
            df_out_lines = df_out_lines.loc[df_out_lines.groupby(['meshblock_id','centre_name','time_of_day'])["total_minutes"].idxmin()]
            dfs.append(df_out_lines)
            del df_out_lines

            #result.export(arcpy.nax.ClosestFacilityOutputDataType.Routes, output_routes)
        else:
            t2 =time.time()
            logger.error(f'solved failed for {timeofday.strftime(dt_format)} for {input_origins.name} and {input_destinations.name}')
            print(result.solverMessages(arcpy.nax.MessageSeverity.All))
            
            pass
    
    te = time.time()
    if dfs:
        df = pd.concat(dfs,axis=0)
        #df = df.loc[df.groupby(['meshblock_id','centre_name','start_time'])["total_minutes"].idxmin()]
        #csvfilename=os.path.join(output_matrix_dir,"matrix_meshblocks_{}_{}_{}.csv".format(*param[0],param[1].replace(" ","_")))
        df.to_csv(csvfilename,index =False)
        logger.debug(f'exported {csvfilename} and {minutes_taken(ts,te)}')
        del dfs
        del df
        return True
    else:
        return False

  
#df cursor

def main():
    ts =time.time()
    
    #setting up paths and change if needed
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_folder = os.path.join(project_root,'data')
    script_folder = os.path.join(project_root,'scripts')
    io_gdb  = os.path.join(data_folder,'io.gdb')
    default_gdb = os.path.join('Default.gdb')
    network_gdb = os.path.join(data_folder,'Gtfs_Oct19.gdb')
    output_dir = os.path.join(project_root,'output')
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    output_matrix_dir = os.path.join(output_dir,'matrix')
    if not os.path.exists(output_matrix_dir):
        os.makedirs(output_matrix_dir)

    env.workspace = default_gdb
    env.overwriteOutput =True

    fc_meshblocks =os.path.join(io_gdb,'meshblocks16')
    fc_stops = os.path.join(io_gdb,'stops')

    csv_centres = os.path.join(script_folder,'centres.csv')
    df_centres= pd.read_csv(csv_centres)
    centrenames = df_centres['centrename'].tolist()

    params = Chunks(fc_meshblocks).by_size(1000)

    logger.debug(f"no of csv will be created: {len(params)}")
    logger.debug(params)

    nds = os.path.join(network_gdb,'SydneyTransitNetwork','TransitNetwork_ND')
    date_range =["2019-11-04 7:00","2019-11-04 9:00"]
    default_cutoff = 30
    logger.debug(f"creating matrix for the period between {date_range[0]} and {date_range[1]} for {default_cutoff} minutes")
    
    my_matrix= partial(calculate_matrix,fc_meshblocks,fc_stops,nds,date_range,default_cutoff,output_matrix_dir)
   

    #change number of worker here
    status =None
 
    try:
        pool = multiprocessing.Pool(processes=6)
        results =[]
        for param in params:
            results.append(pool.apply_async(my_matrix,args=(param,)))
        
        status =[result.get() for result in results]
    except Exception as e:
        logger.error("multiprocessing failed due to {}".format(str(e)))
        import traceback
        tb = sys.exc_info()[2]
        logger.error ("An error occured on line %i" % tb.tb_lineno)
        
    
    finally:
        pool.close()
        pool.join()
    te = time.time()
    message = "completed and {}".format(minutes_taken(ts,te))
    logger.info(message)
    return status
   

 

if __name__=='__main__':
    results=main()
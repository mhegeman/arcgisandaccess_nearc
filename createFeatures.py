"""
Author: Melissa Albino Hegeman, melissa.hegeman@gmail.com
Date: April 2016
Purpose: This script is used to by the shellfish unit to create an ArcGIS feature
class from the Shellfish management database information.
    
"""
def aquacultureCreateConnection():
    
    dbPath = 'K:/MRCD/aquacultureDB/shellfishManagement.accdb'
    accessDriver = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + dbPath

    cnxn = pyodbc.connect(accessDriver)
    return cnxn
 
def getLeaseCoordinates():   
    #query aquaculture db
    query = """SELECT Lease_Info2.LeaseID, Lease_Info2.ShapeType, Coordinates2.Latitude, Coordinates2.Longitude
            FROM Coordinates2 INNER JOIN Lease_Info2 ON Coordinates2.LeaseID = Lease_Info2.LeaseID;"""
    cnxn = aquacultureCreateConnection()
    #create pandas dataframe with data
    sourceData = pandas.read_sql(query, cnxn)
    return sourceData

def createCircles(sourceData, outputFile):
    #arcpy.env.workspace = workspace
    #pick out all the circles
    sourceData = sourceData[sourceData.ShapeType=='Circle']
    
    tempFileWGS84 = r"leaseCircleCenterpoint_WGS84"
    tempFileNAD83 = r"leaseCircleCenterpoint_NAD83"
    template = "aquacultureLeaseTemplate_WGS84"
    #define spatial reference
    #WGS84
    spatialRefWGS84 = arcpy.Describe("aquacultureLeaseTemplate_WGS84").spatialReference
    #NAD83
    spatialRefNAD83 = arcpy.Describe("aquacultureLeaseTemplate_NAD83").spatialReference
    #create temporary point shapefile
    if arcpy.Exists(tempFileWGS84):
        try:
            arcpy.Delete_management(tempFileWGS84)
            
        except: pass
        
    if arcpy.Exists(tempFileNAD83):
        try:
            arcpy.Delete_management(tempFileNAD83)
            
        except: pass
    
    try:
        arcpy.CreateFeatureclass_management(r"C:\Users\maalbino\Documents\GIS\aquaculture\shellfishManagement.gdb",
                                            tempFileWGS84, "POINT", template, "DISABLED", "DISABLED", spatialRefWGS84)
        
    except:
        pass
        
    #create insert cursor
    cursor = arcpy.da.InsertCursor(tempFileWGS84,['SHAPE@XY', 'LeaseID']) 
    
    for x in sourceData.iterrows():
        #returns a tuple: index# and row
        coordPair = x[1]
        LeaseID = coordPair[0]
        lon = coordPair[3]
        lat = coordPair[2]
        point = lon,lat
        
        cursor.insertRow([point,LeaseID])
    del cursor
    
    # Reproject data to NAD83    
    arcpy.Project_management(tempFileWGS84, tempFileNAD83, spatialRefNAD83)
    
    #create temporary point shapefile
    if arcpy.Exists(outputFile):
        try:
            arcpy.Delete_management(outputFile)
            
        except: pass
    #Buffer point file
    arcpy.Buffer_analysis(tempFileNAD83, outputFile, '250 Feet')
    
    if arcpy.Exists(tempFileWGS84):
        try:
            arcpy.Delete_management(tempFileWGS84)
        except: pass
        
    if arcpy.Exists(tempFileNAD83):
        try:
            arcpy.Delete_management(tempFileNAD83)
            
        except: pass


def createPolygons(df, latField, lonField,tempFileWGS84):
    '''The source file is a pandas data frame created from the MS access aquaculture db, 
    latField: the string that defines the field containing latitude(DD),
    lonFiled: the string that defines the field containing longitude(DD),
    Requires pandas, arcpy
    '''
    polygondf = df[df.ShapeType=='Polygon']
        
    template = "aquacultureLeaseTemplate_WGS84"
    #define spatial reference
    #WGS84
    spatialRefWGS84 = arcpy.Describe("aquacultureLeaseTemplate_WGS84").spatialReference
        
    if arcpy.Exists(tempFileWGS84):
        try:
            arcpy.Delete_management(tempFileWGS84)
            
        except: pass
    
    try:
        arcpy.CreateFeatureclass_management(r"C:\Users\maalbino\Documents\GIS\aquaculture\shellfishManagement.gdb",
                                            tempFileWGS84, "Polygon", template, "DISABLED", "DISABLED", spatialRefWGS84)
        
    except: pass
        
    
    cursor = arcpy.da.InsertCursor(tempFileWGS84,['SHAPE@','LeaseID'])
    #get list of unique ids
    uniqueNames = polygondf.LeaseID.unique()
    #for each unique id, create shape
    for leaseID in uniqueNames:
        
        #create a new dataframe by subsetting the original
        #based on the lease id
        newdf = polygondf[polygondf.LeaseID==leaseID]
        #create an empty array
        vertexArray = arcpy.Array()
        
        for x in newdf.iterrows():
            #returns a tuple: index# and row
            coordPair = x[1]
            lon = coordPair[lonField]
            lat = coordPair[latField]
            point = arcpy.Point(lon,lat)
            vertexArray.add(point)
        #get the first point of the polygon to close it
        endPoint = vertexArray[0]
        vertexArray.add(endPoint)
        polygon = arcpy.Polygon(vertexArray,spatialRefWGS84)   
        cursor.insertRow([polygon,leaseID])
        
    del cursor
        
def createAttributesTable(tableName):
    #create table based on template
    tableTemplate = 'aquacultureLeaseTableTemplate'
    if arcpy.Exists(tableName):
        try:
            arcpy.Delete_management(tableName)
        except: pass
    arcpy.CreateTable_management(arcpy.env.workspace, tableName, tableTemplate)
    #query aquaculture db to populate attribute table
    query = """SELECT Lease_Info2.LeaseID, Lease_Info2.Access, Permit_Holders.PermitNum, Permit_Holders.PermitHolder
        FROM Permit_Holders INNER JOIN Lease_Info2 ON Permit_Holders.PermitNum = Lease_Info2.PermitNum;"""
    cnxn = aquacultureCreateConnection()
    tableData = pandas.read_sql(query, cnxn)
    
    cursor = arcpy.da.InsertCursor(tableName,['LeaseID','Program','PermitNum','PermitHolder'])
    
    for x in tableData.iterrows():
        #returns a tuple: index# and row
        row = x[1]
        cursor.insertRow([row[0],row[1],row[2],row[3]])
            
    del cursor



import arcpy
import pandas
import pyodbc

#get coordinate data from aquaculture database
sourceFile = getLeaseCoordinates()

#set up ArcGIS stuff
arcpy.env.workspace = r"C:\Users\maalbino\Documents\GIS\aquaculture\shellfishManagement.gdb"

polytempFileWGS84 = "leasePolygonsWGS84"
polytempFileNAD83 = "leasePolygonsNAD83"
outputFile = "aquacultureLeases_NAD83"
outputTable = "aquacultureAttributeTable"

spatialRefNAD83 = arcpy.Describe("aquacultureLeaseTemplate_NAD83").spatialReference

createCircles(sourceFile,outputFile)

createPolygons(sourceFile,'Latitude','Longitude',polytempFileWGS84)

arcpy.Project_management(polytempFileWGS84, polytempFileNAD83, spatialRefNAD83)
arcpy.Append_management(polytempFileNAD83, outputFile, "NO_TEST")

createAttributesTable(outputTable)
#delete temporary files
if arcpy.Exists(polytempFileWGS84):
    arcpy.Delete_management(polytempFileWGS84)
if arcpy.Exists(polytempFileNAD83):
    arcpy.Delete_management(polytempFileNAD83)

del sourceFile

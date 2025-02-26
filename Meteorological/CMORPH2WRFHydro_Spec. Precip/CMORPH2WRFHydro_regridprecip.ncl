;----------------------------------------------------------------------
; Name    : CMORPH2WRFHydro_regrid.ncl
;
; Purpose : This is an NCL script that uses ESMF utilities to perform regridding of various 
;           data fields to a WRF-Hydro domain.
;
;           Note that before running this script you will need to run the 
;           CMORPH2WRFHydro_generate_weights.ncl script to generate the required weight
;           file(s) for your chosen regridding option(s). 
;
;           Important notes:
;           1) This script assumes that the source files are provided at an hourly interval.
;              This is true for the current generation of CMORPH forcing data and is an assumption made
;              to compute the rainrate variable in output files.          
;           2) This script is currently configured to use the bilinear interpolation option for all forcing 
;              fields.  Should the user desire to use a different interpolation option, the name for the
;              corresponding weight file should be added to user settings and the weight files for each 
;              variable adjusted appropriately.
;           3) By default the domain number for the output filename is set to 1.  This can be changed in the 
;              user settings section of this script.
;
;
; Example Usage   : nohup ncl  CMORPH2WRFHydro_regridprecip.ncl >ncl.log 2>&1 &
; 
;           srcFileName = filename pattern of the souce CMORPH files.
;
;           dstGridName = name of file that contains the destination (WRF-Hydro) grid
; 
; Kyoko Ikeda 23 December 2013
; Updated: Mar 15, 2018   K. FitzGerald
; Updated: Mar 28, 2019   K. FitzGerald
; rewrite: NOV 14, 2024   ShiHuaixuan     To:to fit for CMORPH
;
;----------------------------------------------------------------------
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_code.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/gsn_csm.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/csm/contributed.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/contrib/ut_string.ncl"
load "$NCARG_ROOT/lib/ncarg/nclscripts/esmf/ESMF_regridding.ncl"

begin

;----------------------------------------------------------------------
; User settings
;----------------------------------------------------------------------
  dstGridName="geo_em.d03.nc"
  srcFileName="CMORPH*nc"
  ;---------------------------------------------------------------;
  ; Set the domain number to agree with the geogrid file and      ;
  ; the IGRID option in the hydro.namelist for WRF-Hydro          ;
  ;---------------------------------------------------------------;
  domain  = "DOMAIN3"

  ;---------------------------------------------------------------;
  ; Set input and output file directories.                        ;
  ;---------------------------------------------------------------;
  dirm    = "./input_files/"       ; directory where source forcing data resides
  outdir  = "./output_files/"      ; directory where regridded forcing data will be placed. set to dirm for overwriting the original file

  ;---------------------------------------------------------------;
  ; Weight filenames for regridding                               ;
  ;---------------------------------------------------------------;
  ;wgtFileName_conserve = "./CMORPH2WRFHydro_weight_conserve.nc"
  wgtFileName_bilinear = "./CMORPH2WRFHydro_weight_bilinear.nc"

  ;---------------------------------------------------------------;
  ; Data field names from the source file to be regridded         ;
  ;---------------------------------------------------------------;
   PCP_varname  = "cmorph"
;----------------------------------------------------------------------
; read in source and destination files
;----------------------------------------------------------------------

  if ( .not.isfilepresent( dstGridName ) ) then
     print( " ... source grid file not found : "+ dstGridName )
     exit
  end if

;---  destination grid data
  dstfile     = addfile( dstGridName ,"r")
  dlon2d=dstfile->XLONG_M(0,:,:)
  dlat2d=dstfile->XLAT_M(0,:,:)
  dims=dimsizes(dlat2d)

  flag=0    ; flag for removing accum precip... (should be set to 0 for GLDAS v2.1 forcing data)

;----------------------------------------------------------------------
; Open source data files to be regridded...
;----------------------------------------------------------------------
  system("mkdir "+outdir)

  datfils = systemfunc ("ls "+dirm+srcFileName)    ; list of file names
  num_datfils     = dimsizes(datfils)

  do ifil = 0,num_datfils-1,1   ; loop through datafiles one at a time
    datfile = addfile( datfils(ifil), "r")
    print( " ... Open input file : "+ datfils(ifil) )
   
    ;----------------------------------------------------------------------
    ; Parse time from filename : CMORPH_V1.0_ADJ_8km-1h_2023070100.nc
    ;----------------------------------------------------------------------
    strs = str_split( datfils(ifil), "_" )
    sub_str = strs(5)
    dtime_map = (/4,2,2,2,3/)
    date_vec = str_split_by_length( sub_str, dtime_map)
    print( date_vec )
    date_str = date_vec(0)+date_vec(1)+date_vec(2)+date_vec(3)
    Times_str = date_vec(0)+"-"+date_vec(1)+"-"+date_vec(2)+"_"+date_vec(3)+":00:00"

    print(date_str)
    print(Times_str)
    
    new_string = str_strip(Times_str)
    Times = stringtochar( new_string )
    Times!0 = "DateStrLen"
    print(date_vec)
    valid_time = cd_inv_calendar(toint(date_vec(0)), toint(date_vec(1)), toint(date_vec(2)), toint(date_vec(3)), toint(0) ,toint(0) ,"seconds since 1970-01-01 00:00:00", 0)
    print(valid_time)
    valid_time!0 = "Time"

    ;----------------------------------------------------------------------
    ; Open output file; YYYYMMDDHH00.PRECIP_FORCING.nc
    ;----------------------------------------------------------------------
    ncfile= outdir + date_str+"00.PRECIP_FORCING.nc"
    if ( isfilepresent( ncfile ) ) then
      system ("/bin/rm "+ncfile)
    end if
    ncdf= addfile(ncfile,"c")
    filedimdef( ncdf ,"Time",-1,True)
    ncdf->lat = dlat2d   ;output lat
    ncdf->lon = dlon2d   ;output lon
    ncdf->Times = Times  ;output times
    ncdf->valid_time = valid_time
   
    ;----------------------------------------------------------------------
    ;  Processing Prec ...no further modifications should be required...
    ;----------------------------------------------------------------------
    var = datfile->$PCP_varname$
    wgtFileName = wgtFileName_bilinear 
    var@long_name = "precip"
    var@description = "precipitation"
    var@units       = "mm"
    printVarSummary(var)
    ;----------------------------------------------------------------------
    ; Define new array and perform regriddding
    ;----------------------------------------------------------------------
    ntimes = 1  ; for CMORPH, there is only one time step in each file
    v4 = new((/ntimes(0), dims(0), dims(1)/),"double")
       
    printVarSummary(v4)

    ;---Options to pass to ESMF_regrid--------------------;
     
    opt                = True
    opt@WgtFileName    = wgtFileName
    opt@CopyVarAtts     = True
    opt@CopyVarCoords  = False
     
    ;---Debug information
      opt@PrintTimings   = True
      opt@Debug          = True
    ;-----------------------------------------------------;
    exist = isfilepresent( wgtFileName )
    if ( .not.exist ) then
      print( " ... no weight file. Run GLDAS2WRFHydro_generate_weights.ncl first to generate: "+" "+wgtFileName )
      exit
    end if
     
    var2d = var
    v4 = ESMF_regrid_with_weights( var2d , wgtFileName, opt)
     
    ;----------------------------------------------------------------------
    ; Export interpolated data to new forcing file...
    ;----------------------------------------------------------------------
       
    ;--- change dimension names so that ncks command works appropriately ---;
    v4!0 = "Time"
    v4!1 = "south_north"
    v4!2 = "west_east"
    printVarSummary(v4)

    ncdf->precip = v4

    delete([/var,v4,var2d,wgtFileName/])
    ; end do for prec variable

    ; no need to move files
    ;dstFile = str_sub_str(ncfile, ".nc" , "")
    ;system("mv "+ncfile+" "+dstFile+".nc")
   end do   ; end do for file loop

end

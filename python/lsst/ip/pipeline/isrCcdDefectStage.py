#!/usr/bin/env python

# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log
import lsst.pex.exceptions as pexExcept

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom as cameraGeom
import lsst.afw.geom as afwGeom
import lsst.meas.algorithms as measAlg
import lsst.afw.display.ds9 as ds9
import lsst.afw.display.utils as ds9utils

class IsrCcdDefectStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "CcdDefect -- Parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline",
                "IsrCcdDefectStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing CCD defect mask.")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.ccdExposure"))
        #get known defects from camera class
        defectBaseList = cameraGeom.cast_Ccd(exposure.getDetector()).getDefects()
        id = exposure.getDetector().getId()
        defectList = measAlg.DefectListT()
        #create master list of defects and add those from the camera class
        for d in defectBaseList:
            bbox = d.getBBox()
            nd = measAlg.Defect(bbox)
            defectList.append(nd)
        fwhm = self.policy.getDouble("parameters.defaultFwhm")
        #get saturated pixels from the mask
        sdefects = ipIsr.defectListFromMask(exposure, growFootprints=1, maskName='SAT')
        #mask bad pixels in the camera class
        ipIsr.maskBadPixelsDef(exposure, defectList,
            fwhm, interpolate=False, maskName='BAD')
        #add saturated pixels to master defect list
	for d in sdefects:
            bbox = d.getBBox()
	    nd = measAlg.Defect(bbox)
	    defectList.append(nd)
        #find unmasked bad pixels and mask them
        exposure.getMaskedImage().getMask().addMaskPlane("UNMASKEDNAN")	
        unc = ipIsr.UnmaskedNanCounterF()
        unc.apply(exposure.getMaskedImage())
        nnans = unc.getNpix()
        metadata = exposure.getMetadata()
        metadata.set("NUMNANS", nnans)
        if nnans == 0:
            self.log.log(Log.INFO, "Zero unmasked nans/infs were found, which is good.")
        else:
            self.log.log(Log.INFO, "%i unmasked nans/infs found in ccd exposure: %s"%(nnans, id.__str__()))
        #get footprints of bad pixels not in the camera class
        undefects = ipIsr.defectListFromMask(exposure, growFootprints=0, maskName='UNMASKEDNAN')
	for d in undefects:
            bbox = d.getBBox()
	    nd = measAlg.Defect(bbox)
	    defectList.append(nd)
        #interpolate all bad pixels
	ipIsr.interpolateDefectList(exposure, defectList, fwhm)
        exposure.getMaskedImage().getMask().removeMaskPlane("UNMASKEDNAN")
	
        #output products
        clipboard.put(self.policy.get("outputKeys.defectMaskedCcdExposure"),
                exposure)
        
class IsrCcdDefectStage(harnessStage.Stage):
    parallelClass = IsrCcdDefectStageParallel


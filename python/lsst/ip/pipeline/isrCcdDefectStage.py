#!/usr/bin/env python
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
        defectBaseList = cameraGeom.cast_Ccd(exposure.getDetector()).getDefects()
        id = exposure.getDetector().getId()
        defectList = measAlg.DefectListT()
        for d in defectBaseList:
            bbox = d.getBBox()
            nd = measAlg.Defect(bbox)
            defectList.append(nd)
        fwhm = self.policy.getDouble("parameters.defaultFwhm")
        sdefects = ipIsr.defectListFromMask(exposure, growFootprints=1, maskName='SAT')
        ipIsr.maskBadPixelsDef(exposure, defectList,
            fwhm, interpolate=False, maskName='BAD')
	for d in sdefects:
            bbox = d.getBBox()
	    nd = measAlg.Defect(bbox)
	    defectList.append(nd)
	ipIsr.interpolateDefectList(exposure, defectList, fwhm)
        unc = ipIsr.UnmaskedNanCounterF()
        unc.apply(exposure.getMaskedImage())
        nnans = unc.getNpix()
        metadata = exposure.getMetadata()
        metadata.set("numNans", nnans)
        if nnans == 0:
            self.log.log(Log.INFO, "Zero unmasked nans/infs were found, which is good.")
        else:
            raise pexExcept.LsstException(nnans, \
               "%i unmasked nans/infs found in ccd exposure: %s"%(nnans, id.__str__()))
	
        #output products
        clipboard.put(self.policy.get("outputKeys.defectMaskedCcdExposure"),
                exposure)
        
class IsrCcdDefectStage(harnessStage.Stage):
    parallelClass = IsrCcdDefectStageParallel


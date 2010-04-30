#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom as cameraGeom

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
        defectList = measAlg.DefectList()
        for d in defectBaseList:
            nd = measAlg.Defect(d.getBBox())
            defectList.append(nd)
        ipIsr.maskBadPixelsDef(exposure, defectList,
            self.policy.getDouble("parameters.defaultFwhm"), interpolate=False, maskName='BAD')
        #output products
        clipboard.put(self.policy.get("outputKeys.defectMaskedCcdExposure"),
                exposure)
        
class IsrCcdDefectStage(harnessStage.Stage):
    parallelClass = IsrCcdDefectStageParallel


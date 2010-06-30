#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom as cameraGeom

class IsrCcdAssemblyStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "CcdAssembly -- Parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline",
                "IsrCcdAssemblyStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing CCD assembly.")
        
        #grab exposure from clipboard
        exposureList = clipboard.get(self.policy.getString("inputKeys.exposureList"))
        rmKeys = self.policy.getArray("parameters.deleteFieldsList")
        amp = cameraGeom.cast_Amp(exposureList[0].getDetector())
        ccdId = amp.getParent().getId()
        ccd = cameraGeom.cast_Ccd(amp.getParent())
        exposure = ipIsr.ccdAssemble.assembleCcd(exposureList, ccd, keysToRemove=rmKeys)
                #output products
        clipboard.put(self.policy.get("outputKeys.assembledCcdExposure"),
                exposure)
        
class IsrCcdAssemblyStage(harnessStage.Stage):
    parallelClass = IsrCcdAssemblyStageParallel


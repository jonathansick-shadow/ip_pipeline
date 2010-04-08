#!/usr/bin/env python
import math
import os

import lsst.pex.harness.stage as harnessStage
from lsst.pex.logging import Log
import lsst.pex.policy as pexPolicy

import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom.utils as cameraGeomUtils

class MakeCameraStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        #self.log = Log(self.log, "TransformMetadataStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline",
                "MakeCameraGeomStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Making camera geometry")
        cameraPolicy = clipboard.get(self.policy.getString("inputKeys.cameraPolicy"))
        camera = cameraGeomUtils.makeCamera(cameraPolicy)

        #output products
        clipboard.put(self.policy.get("outputKeys.cameraInfo"),
                    camera)
        
class MakeCameraStage(harnessStage.Stage):
    parallelClass = MakeCameraStageParallel

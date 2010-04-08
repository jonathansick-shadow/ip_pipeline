import os

from lsst.pex.harness.stage import harnessStage
from lsst.pex.logging import Log
import lsst.pex.policy as pexPolicy
from lsst.daf.base import DateTime, PropertySet
from lsst.daf.persistence import LogicalLocation

import lsst.ip.isr.calibDatabase as calibDatabase
import lsst.afw.cameraGeom as cameraGeom

class IdentifyCalibrationProductsStageParallel(harnessStage.ParallelProcessing):
    def setup(self):
        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline",
                "IdentifyCalibrationProductsStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)
        self.bboxKeyword = self.policy.getString("imageBboxKeyword")
        self.biasBboxKeyword = self.policy.getString("biasBboxKeyword")
        self.dataBboxKeyword = self.policy.getString("dataBboxKeyword")
        self.idKeyword = self.policy.getString("ampIdKeyword")
        self.readoutCornerKeyword = self.policy.getString("readoutCornerKeyword")

    def process(self, clipboard):
        metadata = clipboard.get(self.policy.getString("inputKeys.metadata"))
        cdb = clipboard.get(self.policy.getString("inputKeys.calibrationDB"))

        pathPrefix =
        cliboard.get(self.policy.getString("inputKeys.prefixPath"))

        when = DateTime(metadata.get("dateObs"))

        ccdId = metadata.get("ccdId")
        ampId = metadata.get("ampId")
        
        expTime = metadata.get("expTime")
        darkToApply = self.policy.getString("whichdark")
        darkCalibList = cdb.lookup(when, "dark", ccdId, ampId, all=True)
        darkTimeList = []
        for d in darkCalibList:
            darkTimeList.append(d.expTime)
        darkTimeList.sort()
        if darktoapply == "min":
            darkExpTime = darkTimeList[0]
        elif darktoapply == "max":
            darkExpTime = darkTimeList[-1]
        elif darktoapply == "closest":
            minDist = abs(expTime - darkTimeList[0])
            minExpTime = darkTimeList[0]
            for i in xrange(1, len(darkTimeList)):
                dist = abs(expTime - darkTimeList[i])
                if dist < minDist:
                    minDist = dist
                    minExpTime = darkTimeList[i]
            darkExpTime = minExpTime
        else:
            raise RuntimeError, "Unrecognized method for finding dark to apply: " + str(darktoapply)


        biasPath = cdb.lookup(when, "bias", ccdId, ampId)
        darkPath = cdb.lookup(when, "dark", ccdId, ampId,
                expTime=darkExpTime)
        defectPath = cdb.lookup(when, "defect", ccdId, ampId)
        flatPath = cdb.lookup(when, "flat", ccdId, ampId,
                filter=metadata.get("filter"))
#         fringePath = cdb.lookup(when, "fringe", ccdId, ampId,
#                 filter=metadata.get("filter"))
        linearizePath = cdb.lookup(when, "linearize", ccdId, ampId)

        calibData = PropertySet()
        calibData.set("biasPath", os.path.join(pathPrefix, biasPath))
        calibData.set("darkPath", os.path.join(pathPrefix, darkPath))
        calibData.set("defectPath", os.path.join(pathPrefix, defectPath))
        calibData.set("flatPath", os.path.join(pathPrefix, flatPath))
#         calibData.set("fringePath", os.path.join(pathPrefix, fringePath))
        calibData.set("linearizePath", os.path.join(pathPrefix, linearizePath))

        outputKey = self._policy.get("outputKey")
        self.activeClipboard.put(outputKey, calibData)

        self.outputQueue.addDataset(self.activeClipboard)

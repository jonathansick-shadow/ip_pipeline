#!/usr/bin/env python
import math
import lsst.pex.harness.stage as harnessStage

from lsst.pex.logging import Log

import lsst.afw.cameraGeom as cameraGeom
import lsst.afw.image as afwImage
import lsst.meas.algorithms as measAlg
import lsst.afw.display.ds9 as ds9
import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom as cameraGeom

class SimCalibSaturationDefectStageParallel(harnessStage.ParallelProcessing):
    """
    Description:

    Policy Dictionary:

    Clipboard Input:

    ClipboardOutput:
    """
    def setup(self):
        self.log = Log(self.log, "IsrSaturationStage - parallel")

        policyFile = pexPolicy.DefaultPolicyFile("ip_pipeline", "IsrSaturationStageDictionary.paf", "policy")
        defPolicy = pexPolicy.Policy.createPolicy(policyFile, policyFile.getRepositoryPath())

        if self.policy is None:
            self.policy = pexPolicy.Policy()
        self.policy.mergeDefaults(defPolicy)

    def process(self, clipboard):
        """
        """
        self.log.log(Log.INFO, "Doing Saturation correction.")
        
        #grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        exposure = ipIsr.convertImageForIsr(exposure)
        fwhm = self.policy.getDouble("parameters.defaultFwhm")
        amp = cameraGeom.cast_Amp(exposure.getDetector())
        dataBbox = amp.getDataSec(True)
        x = dataBbox.getX0()
        y = dataBbox.getY0()
        height = dataBbox.getY1() - dataBbox.getY0()
        width = dataBbox.getX1() - dataBbox.getX0()
        dl = measAlg.DefectListT()
        defectList = cameraGeom.cast_Ccd(amp.getParent()).getDefects()
        if amp.getId().getIndex()[1] == 1:
            for d in defectList:
                d.shift(-x, -y)
                bbox = d.getBBox()
                if bbox.getX0()-4 > width or bbox.getX1()-4 < 0 or \
                height-bbox.getY0() - 1 > height or height-bbox.getY1() - 1 < 0:
                    pass
                else:
                    nd =\
                    measAlg.Defect(afwImage.BBox(afwImage.PointI(bbox.getX0()+4,
                        height - bbox.getY0()+1), bbox.getHeight(), bbox.getWidth()))
                    dl.append(nd)
        else:
            for d in defectList:
                d.shift(-x, -y)
                bbox = d.getBBox()
                if bbox.getX0()-4 > width or bbox.getX1()-4 < 0 or \
                bbox.getY0()-1 > height or bbox.getY1()-1 < 0:
                    pass
                else:
                    nd =\
                    measAlg.Defect(afwImage.BBox(afwImage.PointI(bbox.getX0()+4,
                        bbox.getY0()+1), bbox.getHeight(), bbox.getWidth()))
                    dl.append(nd)
        saturation = amp.getElectronicParams().getSaturationLevel()
        ipIsr.maskBadPixelsDef(exposure, dl,
                fwhm, interpolate=True, maskName='BAD')
        ipIsr.saturationCorrection(exposure, int(saturation), fwhm,
                growSaturated=False, interpolate=True)
        #ds9.mtv(exposure, frame = 0)
        #exposure.writeFits("Amp.fits")
        #output products
        clipboard.put(self.policy.get("outputKeys.saturationCorrectedExposure"),
                exposure)
        
class SimCalibSaturationDefectStage(harnessStage.Stage):
    parallelClass = SimCalibSaturationDefectStageParallel


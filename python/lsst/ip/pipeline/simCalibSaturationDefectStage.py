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

import lsst.afw.geom as afwGeom
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
        x = dataBbox.getMinX()
        y = dataBbox.getMinY()
        height = dataBbox.getMaxY() - dataBbox.getMinY()
        width = dataBbox.getMaxY() - dataBbox.getMinX()
        dl = measAlg.DefectListT()
        defectList = cameraGeom.cast_Ccd(amp.getParent()).getDefects()
        if amp.getId().getIndex()[1] == 1:
            for d in defectList:
                d1 = measAlg.Defect(d.getBBox())
                d1.shift(-x, -y)
                bbox = d1.getBBox()
                if bbox.getMinX() - 4 > width or bbox.getMaxY() - 4 < 0 or \
                    height - bbox.getMinY() - 1 > height or height - bbox.getMaxY() - 1 < 0:
                    pass
                else:
                    nd = measAlg.Defect(afwGeom.Box2I(
                        afwGeom.Point2I(bbox.getMinX() + 4, height - bbox.getMinY() + 1),
                        bbox.getDimensions()))
                    dl.append(nd)
        else:
            for d in defectList:
                d1 = measAlg.Defect(d.getBBox())
                d1.shift(-x, -y)
                bbox = d1.getBBox()
                if bbox.getMinX() - 4 > width or bbox.getMaxY() - 4 < 0 or \
                    bbox.getMinY() - 1 > height or bbox.getMaxY() - 1 < 0:
                    pass
                else:
                    nd = measAlg.Defect(afwGeom.Box2I(
                        bbox.getMin() + afwGeom.Extent2I(4, 1),
                        bbox.getDimensions()))
                    dl.append(nd)
        saturation = amp.getElectronicParams().getSaturationLevel()
        ipIsr.maskBadPixelsDef(exposure, dl,
                fwhm, interpolate=True, maskName='BAD')
        ipIsr.saturationCorrection(exposure, int(saturation), fwhm,
                growSaturated=False, interpolate=True)
        #ds9.mtv(exposure, frame = 0, title = "my Amp")
        #exposure.writeFits("Amp.fits")
        #output products
        clipboard.put(self.policy.get("outputKeys.saturationCorrectedExposure"),
                exposure)
        
class SimCalibSaturationDefectStage(harnessStage.Stage):
    parallelClass = SimCalibSaturationDefectStageParallel


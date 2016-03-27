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
import lsst.afw.image as afwImage
import lsst.pex.policy as pexPolicy
import lsst.ip.isr as ipIsr
import lsst.afw.cameraGeom as cameraGeom


class IsrSaturationStageParallel(harnessStage.ParallelProcessing):
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

        # grab exposure from clipboard
        exposure = clipboard.get(self.policy.getString("inputKeys.exposure"))
        metadata = exposure.getMetadata()
        exposure = ipIsr.convertImageForIsr(exposure)
        fwhm = self.policy.getDouble("parameters.defaultFwhm")
        amp = cameraGeom.cast_Amp(exposure.getDetector())
        saturation = amp.getElectronicParams().getSaturationLevel()
        bboxes = ipIsr.saturationDetection(exposure, int(saturation), doMask = True)
        self.log.log(Log.INFO, "Found %i saturated regions."%(len(bboxes)))
        # output products
        clipboard.put(self.policy.get("outputKeys.saturationMaskedExposure"),
                      exposure)


class IsrSaturationStage(harnessStage.Stage):
    parallelClass = IsrSaturationStageParallel


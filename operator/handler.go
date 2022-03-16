package operator

import (
	"github.com/gofiber/fiber/v2"
)

type StatusResponse struct {
	ActiveScalingContinues bool
	ScalingDisabled        bool
}

func newScalingCalculationDisable(c *fiber.Ctx) error {
	if scalingManager.ActiveScalingContinues {
		return c.Status(fiber.StatusBadRequest).SendString("The scaling operation continues. After that, you try it !!!")
	}
	scalingManager.ScalingDisabled = true
	return c.SendStatus(fiber.StatusOK)
}

func newScalingCalculationEnable(c *fiber.Ctx) error {
	scalingManager.ScalingDisabled = false
	return c.SendStatus(200)
}

func getScalingStatus(c *fiber.Ctx) error {
	response := StatusResponse{
		ActiveScalingContinues: scalingManager.ActiveScalingContinues,
		ScalingDisabled:        scalingManager.ScalingDisabled,
	}
	return c.Status(200).JSON(response)
}

func SetupRoutes(app *fiber.App) {
	app.Get("/scaling/status", getScalingStatus)
	app.Get("/scaling/disable", newScalingCalculationDisable)
	app.Get("/scaling/enable", newScalingCalculationEnable)
}

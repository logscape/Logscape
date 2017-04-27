On Error Resume Next
strComputer = "."
Set objWMIService = GetObject("winmgmts:\\" & strComputer & "\root\cimv2")
Set colItems = objWMIService.ExecQuery("Select * from Win32_Processor")
For Each objItem in colItems
    'Wscript.Echo "Address Width: " & objItem.AddressWidth
    'Wscript.Echo "Architecture: " & objItem.Architecture
    'Wscript.Echo "Availability: " & objItem.Availability
    'Wscript.Echo "CPU Status: " & objItem.CpuStatus
    'Wscript.Echo "Current Clock Speed: " & objItem.CurrentClockSpeed
    'Wscript.Echo "Data Width: " & objItem.DataWidth
    'Wscript.Echo "Description: " & objItem.Description
    'Wscript.Echo "Device ID: " & objItem.DeviceID
    'Wscript.Echo "Ext Clock: " & objItem.ExtClock
    'Wscript.Echo "Family: " & objItem.Family
    'Wscript.Echo "L2 Cache Size: " & objItem.L2CacheSize
    'Wscript.Echo "L2 Cache Speed: " & objItem.L2CacheSpeed
    'Wscript.Echo "Level: " & objItem.Level
    Wscript.Echo "LoadPercentage:" & objItem.LoadPercentage
    'Wscript.Echo "Manufacturer: " & objItem.Manufacturer
    'Wscript.Echo "Maximum Clock Speed: " & objItem.MaxClockSpeed
    'Wscript.Echo "Name: " & objItem.Name
    'Wscript.Echo "PNP Device ID: " & objItem.PNPDeviceID
    'Wscript.Echo "Processor Id: " & objItem.ProcessorId
    'Wscript.Echo "Processor Type: " & objItem.ProcessorType
    'Wscript.Echo "Revision: " & objItem.Revision
    'Wscript.Echo "Role: " & objItem.Role
    'Wscript.Echo "Socket Designation: " & objItem.SocketDesignation
    'Wscript.Echo "Status Information: " & objItem.StatusInfo
    'Wscript.Echo "Stepping: " & objItem.Stepping
    'Wscript.Echo "Unique Id: " & objItem.UniqueId
    'Wscript.Echo "Upgrade Method: " & objItem.UpgradeMethod
    'Wscript.Echo "Version: " & objItem.Version
    'Wscript.Echo "Voltage Caps: " & objItem.VoltageCaps
Next
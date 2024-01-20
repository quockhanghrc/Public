#copy fixed range from many sheets
Sub copyselection()
    Dim sht As Worksheet
    Dim myselection As String
    Dim lastrow As Integer
    Dim TH_lastrow As Integer
    Dim current As Worksheet
    For Each current In Worksheets
        lastrow = current.Cells(current.Rows.Count, "A").End(xlUp).Row
        myselection = "A7:AW" & lastrow
        TH_lastrow = Worksheets("Sheet1").Cells(Worksheets("Sheet1").Rows.Count, "A").End(xlUp).Row
        TH_lastrow = TH_lastrow + 1
        current.Range(myselection).Copy Worksheets("Sheet1").Range("A" & TH_lastrow)
    Next
End Sub

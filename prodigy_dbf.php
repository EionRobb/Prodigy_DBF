<?php
class Prodigy_DBF {
    private $Filename, $DB_Type, $DB_Update, $DB_Records, $DB_FirstData, $DB_RecordLength, $DB_Flags, $DB_CodePageMark, $DB_Fields, $FileHandle, $FileOpened;
    private $Memo_Handle, $Memo_Opened, $Memo_BlockSize;
	private $CurrentRowNumber;

    private function Initialize() {

        if($this->FileOpened) {
            fclose($this->FileHandle);
        }

        if($this->Memo_Opened) {
            fclose($this->Memo_Handle);
        }

        $this->FileOpened = false;
        $this->FileHandle = NULL;
        $this->Filename = NULL;
        $this->DB_Type = NULL;
        $this->DB_Update = NULL;
        $this->DB_Records = NULL;
        $this->DB_FirstData = NULL;
        $this->DB_RecordLength = NULL;
        $this->DB_CodePageMark = NULL;
        $this->DB_Flags = NULL;
        $this->DB_Fields = array();

        $this->Memo_Handle = NULL;
        $this->Memo_Opened = false;
        $this->Memo_BlockSize = NULL;
		
		$this->CurrentRowNumber = 0;
    }

    public function __construct($Filename, $MemoFilename = NULL) {
        $this->Prodigy_DBF($Filename, $MemoFilename);
    }

    public function Prodigy_DBF($Filename, $MemoFilename = NULL) {
        $this->Initialize();
        $this->OpenDatabase($Filename, $MemoFilename);
    }

    public function OpenDatabase($Filename, $MemoFilename = NULL) {
        $Return = false;
        $this->Initialize();

        $this->FileHandle = fopen($Filename, "r");
        if($this->FileHandle) {
            // DB Open, reading headers
            $this->DB_Type = dechex(ord(fread($this->FileHandle, 1)));
            $LUPD = fread($this->FileHandle, 3);
            $this->DB_Update = ord($LUPD[0])."/".ord($LUPD[1])."/".ord($LUPD[2]);
            $Rec = unpack("V", fread($this->FileHandle, 4));
            $this->DB_Records = $Rec[1];
            $Pos = fread($this->FileHandle, 2);
            $this->DB_FirstData = (ord($Pos[0]) + ord($Pos[1]) * 256);
            $Len = fread($this->FileHandle, 2);
            $this->DB_RecordLength = (ord($Len[0]) + ord($Len[1]) * 256);
            fseek($this->FileHandle, 28); // Ignoring "reserved" bytes, jumping to table flags
            $this->DB_Flags = dechex(ord(fread($this->FileHandle, 1)));
            $this->DB_CodePageMark = ord(fread($this->FileHandle, 1));
            fseek($this->FileHandle, 2, SEEK_CUR);    // Ignoring next 2 "reserved" bytes

            // Now reading field captions and attributes
            while(!feof($this->FileHandle)) {

                // Checking for end of header
                if(ord(fread($this->FileHandle, 1)) == 13) {
                    break;  // End of header!
                } else {
                    // Go back
                    fseek($this->FileHandle, -1, SEEK_CUR);
                }

				$FieldName = fread($this->FileHandle, 11);
                $Field["Name"] = strtolower(substr($FieldName, 0, strpos($FieldName, "\0")));
                $Field["Type"] = fread($this->FileHandle, 1);
                fseek($this->FileHandle, 4, SEEK_CUR);  // Skipping attribute "displacement"
                $Field["Size"] = ord(fread($this->FileHandle, 1));
                fseek($this->FileHandle, 15, SEEK_CUR); // Skipping any remaining attributes
                $this->DB_Fields[] = $Field;
            }

            // Setting file pointer to the first record
            fseek($this->FileHandle, $this->DB_FirstData);

            $this->FileOpened = true;

            // Open memo file, if exists
            if(!empty($MemoFilename) and file_exists($MemoFilename) and preg_match("%^(.+).fpt$%i", $MemoFilename)) {
                $this->Memo_Handle = fopen($MemoFilename, "r");
                if($this->Memo_Handle) {
                    $this->Memo_Opened = true;

                    // Getting block size
                    fseek($this->Memo_Handle, 6);
                    $Data = unpack("n", fread($this->Memo_Handle, 2));
                    $this->Memo_BlockSize = $Data[1];
                }
            }
        }

        return $Return;
    }
	
	public function getFields() {
        if(!$this->FileOpened) {
			return false;
		}
		return $this->DB_Fields;
	}

    public function GetNextRecord($FieldCaptions = false, $ShowDeleted = false) {
        $Return = NULL;
        $Record = array();
		$this->CurrentRowNumber++;
		
        if(!$this->FileOpened) {
            $Return = false;
		} elseif($this->CurrentRowNumber > $this->DB_Records || feof($this->FileHandle)) {
            $Return = NULL;
        } else {
            // File open and not EOF
			if (!$ShowDeleted) {
				while(fread($this->FileHandle, 1) == '*') { // Deleted flag
					fseek($this->FileHandle, $this->DB_RecordLength - 1, SEEK_CUR);
					$this->CurrentRowNumber++;
				}
				if($this->CurrentRowNumber > $this->DB_Records || feof($this->FileHandle)) {
					return NULL;
				}
			} else {
				fseek($this->FileHandle, 1, SEEK_CUR);
			}
            foreach($this->DB_Fields as $Field) {
                $RawData = fread($this->FileHandle, $Field["Size"]);
                // Checking for memo reference
                if(($Field["Type"] == "M" or $Field["Type"] == "G" or $Field["Type"] == "P") and $Field["Size"] == 4) {
					if (!empty($RawData)) {
						// Memo, General, Picture
						$Memo_BO = unpack("V", $RawData);
						if($this->Memo_Opened and $Memo_BO[1] != 0) {
							fseek($this->Memo_Handle, $Memo_BO[1] * $this->Memo_BlockSize);
							$Type = unpack("N", fread($this->Memo_Handle, 4));
							//if(true || $Type[1] == "1") {
								$Len = unpack("N", fread($this->Memo_Handle, 4));
								$Value = rtrim(fread($this->Memo_Handle, $Len[1]), ' ');
							//} else {
							//    // Pictures will not be shown
							//    $Value = "{BINARY_PICTURE}";
							//}
						} else {
							$Value = '';
						}
					} else {
						$Value = '';
					}
                } else if ($Field["Type"] == 'V') {
					// Varchar
					$Len = ord(substr($RawData, -1));
					$Value = substr($RawData, 0, $Len);
                } else if ($Field["Type"] == 'C') {
					// Char
                    $Value = rtrim($RawData, ' ');
                } else if ($Field["Type"] == 'L') {
					// Logical (Boolean)
					$Value = (!empty($RawData) && ($RawData{0} == 'Y' || $RawData{0} == 'T')) ? 1 : 0;
                } else if ($Field["Type"] == 'Y') {
					// Currency
					
					if (false /* speedhack */ && version_compare(PHP_VERSION, '5.6.3') >= 0) {
						$Value = unpack('P', $RawData);
						$Value = $Value[1] / 10000;
					} else {
						list($lo, $hi) = array_values(unpack('V2', $RawData));
						
						// 64-bit compatible PHP shortcut
						if (false /* speedhack */ && PHP_INT_SIZE >= 8) {
							if ($hi < 0) $hi += (1 << 32);
							if ($lo < 0) $lo += (1 << 32);
							$Value = (($hi << 32) + $lo) / 10000;
						} else 
						// No 64-bit magics	
						if ($hi == 0) {
							// No high-byte, no negative flag
							if ($lo > 0) {
								$Value = $lo / 10000;
							} else {
								$Value = bcdiv(sprintf("%u", $lo), 10000, 4);
							}
						} elseif ($hi == -1) {
							// No high-byte, with negative flag
							if ($lo < 0) {
								$Value = $lo / 10000;
							} else {
								// sprintf is 10% faster than bcadd
								$Value = bcdiv(sprintf("%.0f", $lo - 4294967296.0), 10000, 4);
							}
						} else {
							$negativeSign = '';
							$negativeOffset = 0;
							if ($hi < 0)
							{
								$hi = ~$hi;
								$lo = ~$lo;
								$negativeOffset = 1;
								$negativeSign = '-';
							}	
							$hi = sprintf("%u", $hi);
							$lo = sprintf("%u", $lo);
							
							// 4294967296 = 2^32 = bcpow(2, 32)
							$Value = bcdiv($negativeSign . bcadd(bcadd($lo, bcmul($hi, "4294967296")), $negativeOffset), 10000, 4);
						}
					}
                } else if ($Field["Type"] == 'D') {
					// Date
					if ($RawData != '        ') {
						$Value = substr($RawData, 0, 4) . '-' . substr($RawData, 4, 2) . '-' . substr($RawData, 6);
					} else {
						$Value = '1899-12-30';
					}
                } else if ($Field["Type"] == 'I') {
					// Integer
					if (!empty($RawData)) {
						$Value = unpack('V', $RawData);
						$Value = $Value[1];
					} else {
						$Value = 0;
					}
                } else if ($Field["Type"] == 'B') {
					// Double
					$Value = unpack('d', $RawData);
					$Value = $Value[1];
                } else if ($Field["Type"] == 'Q') {
					// VarBinary
					$Len = ord(substr($RawData, -1));
					$Value = substr($RawData, 0, $Len);
                } else if ($Field["Type"] == 'T') {
					// DateTime (Timestamp)
					if (!empty($RawData)) {
						$Value = unpack('V2', $RawData);
						$Date = jdtounix($Value[1]);
						$Time = round($Value[2] / 1000);
						if ($Date === false) {
							$Value = '1899-12-30 ' . gmdate('H:i:s', $Time);
						} else {
							$Value = gmdate('Y-m-d H:i:s', $Date + $Time);
						}
					} else {
						$Value = '1899-12-30 00:00:00';
					}
                } else if ($Field["Type"] == 'N' || $Field["Type"] == 'F' || $Field["Type"] == '+') {
					// Numeric, Float, Autoincrement
					$Value = (float) trim($RawData);
                } else if ($Field["Type"] == '0') {
					// System 'is nullable' column
					continue;
				} else {
					// Unknown type?
					//var_dump($Field); var_dump($RawData); die();
					$Value = trim($RawData);
				}

                if($FieldCaptions) {
                    $Record[$Field["Name"]] = $Value;
                } else {
                    $Record[] = $Value;
                }
            }

            $Return = $Record;
        }

        return $Return;
    }

    function __destruct() {
        // Cleanly close any open files before destruction
        $this->Initialize();
    }
}

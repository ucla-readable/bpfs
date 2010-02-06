// This file contains an ISA-portable PIN tool for tracing BPFS writes to BPRAM.

// TODO: see source/tools/ManualExamples/proccount.cpp for obtaining fn names

#define __STDC_FORMAT_MACROS

#include <stdio.h>
#include <inttypes.h>
#include "pin.H"


#define BPRAM_INFO "inform_pin_of_bpram"


const void *bpram_start;
const void *bpram_end;

UINT64 nbytes;

#define LOG_WRITES 0

#if LOG_WRITES
KNOB<string> KnobOutputFile(KNOB_MODE_WRITEONCE, "pintool",
	"o", "bpramcount.out", "specify output file name");
FILE *trace;
#endif


VOID RecordMemWrite(VOID *ip, VOID *addr, ADDRINT size)
{
	if (bpram_start <= addr && addr < bpram_end)
	{
		nbytes += size;
#if LOG_WRITES
		// TODO: Log to memory instead of file; output to file at exit.
		//       Perhaps structure the stats per IP, rather than as a log.
	    fprintf(trace,"%p: %p %zu\n", ip, addr, size);
#endif
	}
}

VOID Instruction(INS ins, VOID *v)
{
    // Checking !INS_IsIpRelWrite() does not seem to affect performance
    if (INS_IsMemoryWrite(ins) && !INS_IsStackWrite(ins))
    {
        // The Pin manual suggests dividing this into If and Then pieces
        // to permit inlining of the If case, but I've found that If-Then
        // is slower. Maybe if RecordMemWrite() becomes more expensive
        // this tradeoff will change?
        INS_InsertPredicatedCall(
            ins, IPOINT_BEFORE, (AFUNPTR) RecordMemWrite,
            IARG_INST_PTR,
            IARG_MEMORYWRITE_EA,
            IARG_MEMORYWRITE_SIZE,
            IARG_END);
    }
}

VOID Fini(INT32 code, VOID *v)
{
	printf("pin: %" PRIu64 " bytes written to BPRAM\n", nbytes);
#if LOG_WRITES
	fprintf(trace, "number of bytes written: %" PRIu64 "\n", nbytes);
    fclose(trace);
#endif
}

VOID InformPinBpramBefore(ADDRINT addr, ADDRINT size)
{
	printf("pin: detected %zu MiB (%zu bytes) of BPRAM\n",
	       size / (1024 * 1024), size);
#if LOG_WRITES
	fprintf(trace, "detected %zu MiB (%zu bytes) of BPRAM @ %p\n",
	       size / (1024 * 1024), size, (void*) addr);
#endif
	bpram_start = (void*) addr;
	bpram_end = (void*) (addr + size);
}

VOID Image(IMG img, VOID *v)
{
	// Detect the address and size of BPRAM by inspecting a call to
	// BPRAM_INFO().
	// Alternatively, we could require debug symbols and lookup 'bpram' and
	// 'bpram_size' and either detect when their contents change, to get
	// their post-init values, or watch for a known function call made
	// after bpram is inited but before fuse starts (eg fuse_mount()).
	RTN rtn = RTN_FindByName(img, BPRAM_INFO);
	if (RTN_Valid(rtn))
	{
		RTN_Open(rtn);
		RTN_InsertCall(rtn, IPOINT_BEFORE, (AFUNPTR) InformPinBpramBefore,
			IARG_FUNCARG_ENTRYPOINT_VALUE, 0,
			IARG_FUNCARG_ENTRYPOINT_VALUE, 1,
			IARG_END);
		RTN_Close(rtn);
	}
}

int main(int argc, char **argv)
{
	PIN_InitSymbols();
    PIN_Init(argc, argv);

#if LOG_WRITES
    trace = fopen(KnobOutputFile.Value().c_str(), "w");
#endif

	IMG_AddInstrumentFunction(Image, 0);
    INS_AddInstrumentFunction(Instruction, 0);
    PIN_AddFiniFunction(Fini, 0);

    PIN_StartProgram(); // does not return
    return 0;
}

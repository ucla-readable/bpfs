/* This file is part of BPFS. BPFS is copyright 2009-2010 The Regents of the
 * University of California. It is distributed under the terms of version 2.1
 * of the GNU LGPL. See the file LICENSE for details. */

// This file contains an ISA-portable PIN tool for tracing BPFS writes to BPRAM.

// TODO: see source/tools/ManualExamples/proccount.cpp for obtaining fn names

#define __STDC_FORMAT_MACROS

#include <stdio.h>
#include <string.h>
#include <inttypes.h>
#include "pin.H"

#if __GNUC__ > 4 || (__GNUC__ == 4 && __GNUC_MINOR__ >= 4)
# include <unordered_map>
using std::unordered_map;
#else
# include <ext/hash_map>
# define unordered_map __gnu_cxx::hash_map
#endif

#define BPRAM_INFO "inform_pin_of_bpram"

// Max backtrace depth
#define NBSTEPS 20

// Whether to log each write
#define LOG_WRITES 0


const void *bpram_start;
const void *bpram_end;

UINT64 nbytes;

FILE *trace;

KNOB<string> KnobOutputFile(KNOB_MODE_WRITEONCE, "pintool",
	"o", "bpramcount.out", "specify output file name");

KNOB<string> KnobBacktrace(KNOB_MODE_WRITEONCE, "pintool",
	"b", "false", "specify whether to log write backtraces: true/false");


//
// Log the number of bytes written to BPRAM

VOID RecordMemWrite(VOID *addr, ADDRINT size)
{
	if (bpram_start <= addr && addr < bpram_end)
	{
		nbytes += size;
#if LOG_WRITES
		// TODO: Log to memory instead of file; output to file at exit.
		//       Perhaps structure the stats per IP, rather than as a log.
	    fprintf(trace,"%zu B to %p\n", size, addr);
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
            IARG_MEMORYWRITE_EA,
            IARG_MEMORYWRITE_SIZE,
            IARG_END);
    }
}


//
// Log the number of bytes written to BPRAM and the backtrace for each write

struct backtrace
{
	backtrace() { memset(ips, 0, sizeof(ips)); }
	void *ips[NBSTEPS];
};

bool operator==(const backtrace &bt1, const backtrace &bt2)
{
	return !memcmp(bt1.ips, bt2.ips, sizeof(bt1.ips));
}

struct backtrace_hash : public std::unary_function<backtrace,size_t>
{
	size_t operator()(const backtrace &b) const
	{
		// uses FNV hash taken from Anvil from stl::tr1::hash
		size_t r = 2166136261u;
		for (size_t i = 0; i < NBSTEPS; i++)
		{
			r ^= reinterpret_cast<uintptr_t>(b.ips[i]);
			r *= 16777619;
		}
		return r;
	}
};

typedef unordered_map<backtrace,UINT64,backtrace_hash> backtrace_writes;

backtrace_writes bt_writes;


ADDRINT BpramWriteIf(VOID *addr)
{
	return (bpram_start <= addr && addr < bpram_end);
}

#ifdef __i386__
# define REG_BP_ARCH REG_EBP
#elif defined(__x86_64__)
# define REG_BP_ARCH REG_RBP
#endif

struct stack_frame
{
	struct stack_frame *next;
	void *ret;
};

VOID RecordMemWriteBacktrace(CONTEXT *ctxt, VOID *rip, ADDRINT size)
{
	const char *btopt = "(Might this be because you are trying to backtrace optimized code?)";
	struct stack_frame *fp = reinterpret_cast<struct stack_frame*>(PIN_GetContextReg(ctxt, REG_BP_ARCH));
	struct stack_frame *last_fp = NULL;
	backtrace bt;
	int i = 0;

	nbytes += size;

	bt.ips[0] = reinterpret_cast<void*>(PIN_GetContextReg(ctxt, REG_INST_PTR));

	// Normally rip contains numbers that are small and not in a function.
	// But sometimes REG_INST_PTR (aka EIP) is bogus and rip is not.
	if (rip)
		bt.ips[++i] = rip;

	while (fp >= last_fp && i < NBSTEPS)
	{
		void *ret;
		size_t n;
		EXCEPTION_INFO ei;

		n = PIN_SafeCopyEx(&ret, &fp->ret, sizeof(ret), &ei);
		if (!n)
		{
			printf("pin: stack trace failed at depth %d (read ret)\n", i);
			printf("%s\n", btopt);
			printf("EI: \"%s\"\n", PIN_ExceptionToString(&ei).c_str());
			break;
		}
		if (!ret)
			break;
		bt.ips[++i] = ret;
		last_fp = fp;

		n = PIN_SafeCopyEx(&fp, &last_fp->next, sizeof(fp), &ei);
		if (!n)
		{
			printf("pin: stack trace failed at depth %d (read next)\n", i);
			printf("%s\n", btopt);
			printf("EI: \"%s\"\n", PIN_ExceptionToString(&ei).c_str());
			break;
		}
	}

	backtrace_writes::iterator it = bt_writes.find(bt);
	if (it != bt_writes.end())
		it->second += size;
	else
		bt_writes[bt] = size;
}

VOID InstructionWithBacktrace(INS ins, VOID *v)
{
    // Checking !INS_IsIpRelWrite() does not seem to affect performance
    if (INS_IsMemoryWrite(ins) && !INS_IsStackWrite(ins))
    {
        INS_InsertIfPredicatedCall(
            ins, IPOINT_BEFORE, (AFUNPTR) BpramWriteIf,
            IARG_MEMORYWRITE_EA,
            IARG_END);
        INS_InsertThenPredicatedCall(
            ins, IPOINT_BEFORE, (AFUNPTR) RecordMemWriteBacktrace,
			IARG_CONTEXT,
			IARG_RETURN_IP,
            IARG_MEMORYWRITE_SIZE,
            IARG_END);
    }
}


//
// General

VOID Fini(INT32 code, VOID *v)
{
	printf("pin: %" PRIu64 " bytes written to BPRAM\n", nbytes);

	if (trace)
	{
		fprintf(trace, "total number of bytes written: %" PRIu64 "\n", nbytes);

		fprintf(trace, "write backtraces start:\n");
		for (backtrace_writes::const_iterator it = bt_writes.begin();
		     it != bt_writes.end();
		     ++it)
		{
			fprintf(trace, "%" PRIu64, it->second);
			for (int i = 0; i < NBSTEPS && it->first.ips[i]; i++)
				fprintf(trace, " %p", it->first.ips[i]);
			fprintf(trace, "\n");
		}
		fprintf(trace, "write backtraces end\n");

		fclose(trace);
	}
}

VOID InformPinBpramBefore(ADDRINT addr, ADDRINT size)
{
	printf("pin: detected %zu MiB (%zu bytes) of BPRAM\n",
	       size / (1024 * 1024), size);
	if (trace)
		fprintf(trace, "detected %zu MiB (%zu bytes) of BPRAM @ %p\n",
		        size / (1024 * 1024), size, (void*) addr);
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

	IMG_AddInstrumentFunction(Image, 0);
	if (KnobBacktrace.Value() == "true")
	{
		trace = fopen(KnobOutputFile.Value().c_str(), "w");
		if (!trace)
			fprintf(stderr, "pin: unable to open trace file\n");
		INS_AddInstrumentFunction(InstructionWithBacktrace, 0);
	}
	else
		INS_AddInstrumentFunction(Instruction, 0);
#if LOG_WRITES
	if (!trace)
	{
		trace = fopen(KnobOutputFile.Value().c_str(), "w");
		if (!trace)
			fprintf(stderr, "pin: unable to open trace file\n");
	}
#endif
    PIN_AddFiniFunction(Fini, 0);
	if (trace)
	{
		printf("pin: logging to %s\n", KnobOutputFile.Value().c_str());
		fflush(stdout);
	}

    PIN_StartProgram(); // does not return
    return 0;
}

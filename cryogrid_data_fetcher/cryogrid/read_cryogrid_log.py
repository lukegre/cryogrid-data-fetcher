import numpy as np
import pandas as pd
from memoization import cached
from .. import logger


def read_cryogrid_log(fname, make_summary=False):
    
    raw = read_raw_log(fname)
    log = pd.DataFrame(raw, columns=['line', 'thread', 'message'])
    log = assign_gridcell(log)

    if make_summary:
        summary_fname = '.'.join(fname.split('.')[:-1]) + '_summary.md'
        make_log_summary(log, fname=summary_fname)
        logger.success(f"Summary written to {summary_fname}")
        show_markdown_file_as_html(summary_fname)
    
    return log


@cached(max_size=1)
def read_raw_log(fname, n_lines=1e10):
    import re

    dd_mmm_yyyy = r'\d{2}-[A-Za-z]{3}-\d{4}'

    c = 0
    lines = []
    previous_line = ' '
    with open(fname, 'r') as f:
        for i, line in enumerate(f):

            if i >= n_lines:
                break
            # remove date lines (leaving worker + other lines + empty lines)
            if any(re.findall(dd_mmm_yyyy, line)):
                continue
            # remove worker lines (leaving empty + other lines)
            elif line.startswith('Worker'):
                previous_line = line.replace('\n', '').replace(':', '').replace('  ', ' ').strip().lower()
                continue
            else:  # processing other lines
                line = line[:-1].replace('\n', '')
                
                # removes blank lines (leaving other lines)
                if len(line.replace(' ', '')) == 0:
                    continue
                
                if previous_line.startswith('worker'):
                    # if previous line is worker, append to current line (leaving only lines not preceded by worker)
                    line = [i, previous_line, line]
                    lines.append(line)
                else:
                    # now, append other lines (assume metadata)
                    lines.append([i, 'meta', line])

    return lines


def _get_gridcell(df):
    pattern = r'running grid cell (\d+)'
    gridcell_arr = (
        df.message
        .str.extract(pattern)[0]
        .rename('gridcell')
        .ffill()
        .astype(int)
        .values)
    
    return gridcell_arr
    

def _assign_gridcell(df):
    df = df.sort_values('line')
    gridcell_arr = _get_gridcell(df)
    df = df.assign(gridcell=gridcell_arr).set_index('gridcell')
    return df


@cached(max_size=1)
def assign_gridcell(df):
    import pandas as pd
    idx_workers = df.thread.str.startswith('work')
    idx_other = ~idx_workers

    workers = df[idx_workers].groupby('thread')
    df_workers = (
        workers
        .apply(_assign_gridcell, include_groups=False)
        .reset_index()
        .set_index('gridcell'))
    
    df_other = df[idx_other].assign(gridcell=0).set_index('gridcell')

    df = pd.concat([df_other, df_workers], axis=0)
    df = df.reset_index().sort_values(['gridcell', 'line']).set_index('gridcell')
    return df


def check_run(df_run):
    import pandas as pd
    pattern = r'running tile number (\d+)'

    df = df_run

    tiles = df.message.str.extract(pattern).dropna().astype(int)
    stats = pd.Series(dict(
        line_0 = df.line.iloc[0],
        line_n = df.line.iloc[-1],
        thread = df.thread.iloc[0],
        n_steps = len(df),
        n_tiles = len(tiles),
        max_tile = np.max(tiles), 
        n_lookup = df.message.str.contains('look-up').sum(),
        n_downscale = df.message.str.contains('downscaling').sum(),
    ))

    return stats


def get_worker_summary(df):
    run_summary = (
        df.loc[1:]  # exclude 0 (metadata)
        .groupby('gridcell')  # group by gridcell
        .apply(check_run)  
        .sort_values('line_0'))
    
    run_summary = run_summary.assign(
        count = lambda x: np.arange(len(x)) + 1,
        success = lambda x: x.n_tiles.max() == x.max_tile,
    )

    return run_summary


def get_metadata_str(log):
    meta = log[log.thread == 'meta']

    spacer = ' ' * 5

    n_digits = int(np.ceil(np.log10(meta.line.max())))
    line_num = meta['line'].astype(str).str.zfill(n_digits)

    line_meta = line_num + spacer + meta['message']

    str_out = "LINE   MESSAGE\n"
    str_out += '\n'.join(line_meta)
    
    return str_out


def make_log_summary(log, faulty_first=True, fname=None):

    def make_section_name(name, width=120, char='#', n_leading_chars=2, n_spacer=2):
        name_len = len(name)

        leading = char * n_leading_chars
        spacer = ' ' * n_spacer
        
        len_trailing = width - name_len - n_leading_chars - n_spacer * 2
        trailing = char * len_trailing

        s0 = char * width + '\n'
        s1 = f"{leading}{spacer}{name}{spacer}"
        s2 = '\n' + char * width
        section = f"{s0}{s1}{s2}\n"

        return f"\n{section}\n"
    
    worker_summary = get_worker_summary(log)

    if faulty_first:
        worker_summary = worker_summary.sort_values(['success', 'line_0'], ascending=True)

    info = (
        "This is a summary of the CryoGrid output run on HPC (multiple workers). "
        "The the log file is parsed without reading the dates. Only the more general "
        "information is retained. This file contains a summary of the log for each TILE run. "
        "There are three sections:  \n\n"
        "1. **TILE RUN SUMMARY:**  total, succeeded, and failed runs\n"
        "2. **METADATA:** contains anything that is not run progress - line numbers are also given. \n"
        "3. **TILE RUN INFO:** summary table of each TILE run with the last row indicating success or\n\n"
    )
    worker_summary_str = worker_summary.to_markdown()
    meta_str = f"```\n{get_metadata_str(log)}\n```"

    table_width = len(worker_summary_str.split('\n')[0])
    order = "first" if faulty_first else "last"

    success_failed = pd.Series({
        "TILES Run": len(worker_summary),
        "TILES Successful": worker_summary.success.sum(), 
        "TILES Faulty": (~worker_summary.success).sum()},
        name='Number of tiles',
    ).to_markdown()

    s0 = f"# CRYOGRID LOG SUMMARY"
    s1 = f"## TILE RUN SUMMARY"
    s2 = f'## METADATA'
    s3 = f'## TILE RUN INFO (failed runs {order} - see last column)'

    out = [s0, info, s1, success_failed, s2, meta_str, s3, worker_summary_str]
    out = '\n\n'.join(out) 
    
    if fname is not None:
        with open(fname, 'w') as f:
            f.write(out)
    else:
        return out
    

def show_markdown_file_as_html(fname):
    from IPython.display import display_markdown

    with open(fname, 'r') as f:
        md = f.read()

    display_markdown(md, raw=True)
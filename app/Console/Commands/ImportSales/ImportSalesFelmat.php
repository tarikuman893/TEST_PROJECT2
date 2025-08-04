<?php

namespace App\Console\Commands\ImportSales;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Spatie\SimpleExcel\SimpleExcelReader;

class ImportSalesFelmat extends Command
{
    /** コマンド名 */
    protected $signature   = 'import:sales-felmat';

    /** 説明 */
    protected $description = 'Felmat 売上 CSV を csv_felmat へ取込む';

    // ---------- ヘルパ ----------
    /** 文字列日付 → Y-m-d H:i:s / null */
    private static function toDate(?string $v): ?string
    {
        $v = trim($v ?? '');
        if ($v === '' || $v === '0000-00-00 00:00:00') {
            return null;
        }
        $ts = @strtotime(str_replace('/', '-', $v));
        return $ts === false ? null : date('Y-m-d H:i:s', $ts);
    }

    /** 金額文字列（カンマ・小数点込）→ 整数（円） */
    private static function toInt(?string $v): int
    {
        if ($v === null || $v === '') return 0;
        return (int) floor((float) str_replace(',', '', $v));
    }

    // ---------- 取込 ----------
    public function handle(): int
    {
        $sourceDir  = storage_path('app/csv/Sales');
        $csvPattern = "$sourceDir/*felmat*.csv";
        $headers    = ['成果番号'];   // ヘッダー行判定用

        foreach (glob($csvPattern) as $csv) {
            // SJIS → UTF-8 変換（BOM 保護）
            $tmp = tempnam(sys_get_temp_dir(), 'csv_') . '.csv';
            file_put_contents(
                $tmp,
                mb_convert_encoding(file_get_contents($csv), 'UTF-8', 'SJIS-win,CP932,UTF-8')
            );

            SimpleExcelReader::create($tmp)
                ->useDelimiter(',')
                ->noHeaderRow()
                ->getRows()
                ->reject(function ($row) use ($headers) {
                    $first = $row[0] ?? '';

                    // ①空行 ②ヘッダー行 ③order_id が数値でない行 は取り込まない
                    return (
                        (is_array($row) && count(array_filter($row, fn($v) => $v !== '')) === 0) ||
                        in_array($first, $headers, true) ||
                        !ctype_digit($first)
                    );
                })
                ->each(function (array $row) {
                    DB::table('csv_felmat')->insert([
                        'order_id'         => (int) $row[0],                           // 成果番号（必ず整数）
                        'click_time'       => self::toDate($row[1]  ?? null),
                        'occur_time'       => self::toDate($row[2]  ?? null),
                        'fix_time'         => self::toDate($row[3]  ?? null),
                        'pg_id'            => $row[4]  !== '' ? (int) $row[4] : null,
                        'pg_name'          => $row[5]  ?? null,
                        'ad_type'          => $row[6]  ?? null,
                        'ad_id'            => $row[7]  ?? null,                        // 文字列型に変更済み
                        'ad_picsize'       => $row[8]  ?? null,
                        'reward'           => self::toInt($row[9]  ?? null),
                        'statement'        => $row[10] ?? null,
                        'device'           => $row[11] ?? null,
                        'os'               => $row[12] ?? null,
                        'site_id'          => $row[13] !== '' ? (int) $row[13] : null,
                        'site_name'        => $row[14] ?? null,
                        'referer'          => $row[15] ?? null,
                        'param_1'          => $row[16] ?? null,
                        'is_external'      => $row[17] ?? null,
                        'search_engine'    => $row[18] ?? null,
                        'search_word'      => $row[19] ?? null,
                        'search_lpid'      => $row[20] ?? null,
                        'contracted_state' => $row[21] ?? null,
                        'reward_id'        => $row[22] ?? null,
                        'reward_name'      => $row[23] ?? null,
                    ]);
                });

            unlink($tmp);
        }

        $this->info('Felmat 売上データ取込完了');
        return Command::SUCCESS;
    }
}

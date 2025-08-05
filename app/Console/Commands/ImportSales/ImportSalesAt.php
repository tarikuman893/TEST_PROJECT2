<?php

namespace App\Console\Commands\ImportSales;

use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;

class ImportSalesAt extends Command
{
    protected $signature   = 'import:sales-at';
    protected $description = 'AT 売上 CSV を csv_at へ取込む';

    /* ---------- ヘルパ ---------- */

    /** Y/m/d H:i:00 へ統一 */
    private static function toDate(?string $v): ?string
    {
        if ($v === null || $v === '' || $v === '−') {
            return null;
        }
        $ts = @strtotime($v);
        return $ts === false ? null : date('Y/m/d H:i:00', $ts);
    }

    /** カンマ付き数値 → int（税抜） */
    private static function toInt(?string $v): int
    {
        if ($v === null || $v === '') {
            return 0;
        }
        return (int) str_replace(',', '', $v);
    }

    /** 先頭 BOM を除去 */
    private static function removeBom(string $s): string
    {
        return preg_replace('/^\x{FEFF}/u', '', $s);
    }

    public function handle(): int
    {
        $srcDir     = storage_path('app/csv/Sales');
        $csvFiles   = glob("$srcDir/*at*_sales.csv");
        $hasReferer = Schema::hasColumn('csv_at', 'referer');

        foreach ($csvFiles as $csv) {
            // ‥‥モデルと同じ一時ファイル作成＋エンコード＋BOM除去
            $buffer = file_get_contents($csv);
            $utf8   = mb_convert_encoding($buffer, 'UTF-8', 'auto, SJIS-win, CP932');
            $clean  = self::removeBom($utf8);

            $tmp = tempnam(sys_get_temp_dir(), 'csv_at_') . '.csv';
            file_put_contents($tmp, $clean);

            // SplFileObject で読み込んでモデルと同じルールで処理
            $file = new \SplFileObject($tmp);
            $file->setFlags(\SplFileObject::READ_CSV);
            $rowCount = 0;

            while (! $file->eof()) {
                $row = $file->fgetcsv();

                // fgetcsv が返す false や NULL は無視
                if (! is_array($row)) {
                    continue;
                }

                // 空行なら終了
                if (count($row) === 1 && $row[0] === null) {
                    continue;
                }

                $rowCount++;

                // 1行目は必ずスキップ
                if ($rowCount === 1) {
                    continue;
                }

                // 第1セルから BOM/引用符除去して、ヘッダー「クリック日時」を検出したらスキップ
                $first = (string) ($row[0] ?? '');
                $first = self::removeBom($first);
                $first = trim($first, '"');
                if ($first === 'クリック日時') {
                    continue;
                }

                // 空セルのみの行はスキップ
                if (empty(array_filter($row))) {
                    continue;
                }

                // データ整形
                $data = [
                    'click_time' => self::toDate($row[0]  ?? null),
                    'occur_time' => self::toDate($row[1]  ?? null),
                    'fix_time'   => self::toDate($row[2]  ?? null),
                    'statement'  => $row[3]   ?? null,
                    'pg_id'      => $row[4]   ?? null,
                    'pg_name'    => ($row[5] ?? '') . ($row[16] ?? ''),
                    'ad_id'      => $row[6]   ?? null,
                    'ad_name'    => $row[7]   ?? null,
                    'subject'    => $row[8]   ?? null,
                    'sales'      => self::toInt($row[9]  ?? null),
                    'reward'     => self::toInt($row[10] ?? null),
                    'device'     => $row[12]  ?? null,
                    'pb_id'      => $row[13]  ?? null,
                    'rk'         => $row[14]  ?? null,
                    'site_id'    => $row[15]  ?? null,
                    'site_name'  => $row[16]  ?? null,
                    'rank'       => $row[17]  ?? null,
                ];

                if ($hasReferer && isset($row[11])) {
                    $data['referer'] = $row[11];
                }

                DB::table('csv_at')->insert($data);
            }

            // 一時ファイル削除
            @unlink($tmp);
        }

        $this->info('AT 売上データ取込完了');
        return Command::SUCCESS;
    }
}
